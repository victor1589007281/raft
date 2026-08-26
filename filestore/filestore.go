// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

// Package filestore is vraft's library-level file-backed LogStore +
// StableStore: an append-only, fsync/fdatasync-per-batch raft log with the
// 12.7/12.9/12.14 write-path capabilities behind explicit switches (all
// default off, probed with fallback):
//
//   - SingleWAL unified segment format (v2): raft log record IS the redo
//     record — self-describing file magic, per-record CRC32C torn-tail
//     detection, native [base|lsn] header fields, batch-tail 512B padding
//     (O_DIRECT prerequisite), sparse LSN→(index,off) index.
//   - RedoCodec injection: vraft never interprets redo semantics; the
//     business injects Split/Merge so base/lsn extraction (LogDB redo
//     envelope) lives outside the library. Nil codec keeps payload = Data.
//   - 12.9 kernel I/O knobs: fdatasync, fallocate preallocation,
//     sync_file_range writeback kick, fadvise, io_uring WRITE→FSYNC chain.
//
// On-disk auto-detection: the file format follows the FILE, not the flags —
// a v2 file is always read as v2 (magic), a legacy [u32][msgpack] file stays
// legacy even if SingleWAL is enabled, so flipping switches never corrupts
// existing data.
package filestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

var msgpackHandle codec.MsgpackHandle

// ---------------------------------------------------------------------------
// RedoCodec: business-injected redo envelope semantics (12.14 P2 hook).
//
// vraft treats Log.Data as opaque; the codec decides which part is the redo
// envelope (base/lsn) and which part is the payload stored after the raft
// msgpack header. LogDB injects a parser for its [base|lsn|len|data] /
// JSON {"base":..} envelopes; nil codec means base=lsn=0, payload=Data.
type RedoCodec interface {
	// Split extracts (base, lsn, payload) from l. lsn==0 means "no LSN",
	// the record is skipped by the sparse index.
	Split(l *raft.Log) (base, lsn uint64, payload []byte)
	// Merge rebuilds the original Data from envelope fields and payload.
	Merge(base, lsn uint64, payload []byte) []byte
}

// IndexLSNCodec is a demo/test codec: stamps lsn = raft index so the sparse
// index is fully exercisable without a real redo consumer (LookupByLSN(i)=i).
// The payload is Data itself; Merge is the identity.
type IndexLSNCodec struct{}

func (IndexLSNCodec) Split(l *raft.Log) (uint64, uint64, []byte) { return 0, l.Index, l.Data }
func (IndexLSNCodec) Merge(_, _ uint64, payload []byte) []byte   { return payload }

// RedoSegment is one redo record's worth of payload, addressed by LSN.
type RedoSegment struct {
	LSN     uint64
	Payload []byte
}

// RedoSegmentCodec is the multi-segment extension of RedoCodec for ref mode:
// one raft entry may fold N redo segments (e.g. LogDB's WriteRedoBatcher
// cmdBatch). SplitSegments returns 0 segments for non-redo entries (they are
// stored inline in the meta file). SplitSegments/MergeSegments must round-trip
// Data byte-identically.
type RedoSegmentCodec interface {
	RedoCodec
	SplitSegments(data []byte) (base uint64, segs []RedoSegment)
	MergeSegments(base uint64, segs []RedoSegment) []byte
}

// Mode selects the on-disk store layout for NEW files (existing files always
// keep their own format — auto-detected).
type Mode string

const (
	ModeLegacy  Mode = ""        // [u32][msgpack(Log)] — upstream-compatible
	ModeUnified Mode = "unified" // v2 single file: meta header + payload inline
	ModeRef     Mode = "ref"     // v2 dual file: raft.log = pointer meta, redo.log = data (12.14.3)
)

// ---------------------------------------------------------------------------
// Options: every knob defaults to off (zero value) — a zero Options is the
// upstream-equivalent legacy store.
type Options struct {
	// 12.7/12.14 single WAL.
	SingleWAL   bool       // write v2 unified format on NEW files (= ModeUnified)
	Mode        Mode       // explicit layout; overrides SingleWAL when set
	SparseIndex bool       // build LSN→(index,off) index (effective on v2/v1 files)
	Codec       RedoCodec  // nil = no redo envelope semantics
	Framer      RedoFramer // ref mode: redo record framing; nil = StdFramer

	// 12.9 kernel I/O knobs (probed with fallback).
	Fdatasync           bool  // Sync → Fdatasync (skip inode mtime flush)
	PreallocateSegments bool  // fallocate next segment eagerly
	SegmentSize         int64 // 0 = 64 MiB
	UseFadvise          bool  // DONTNEED truncated tail
	UseSyncFileRange    bool  // async writeback kick mid-batch
	IORing              bool  // io_uring single-enter chain (dual-fd in ref mode)
	IORingSQPoll        bool  // requires IORing
	DirectIO            bool  // O_DIRECT tier (v2 formats only; probed, falls back to buffered)

	// UnboundRedoLimit (12.14 P3): max bytes written via WriteRedoDirect but
	// not yet bound by a committed raft entry; 0 = unlimited. Over the limit
	// the direct write gets ErrUnboundFull (backpressure → caller degrades).
	UnboundRedoLimit int64

	Logger *log.Logger // optional; nil = silent
}

// Validate enforces the switch interlocks (mirrors raft.Config.Validate).
// SparseIndex is deliberately NOT tied to SingleWAL here: the format follows
// the file, so a v2 file must be indexable even when the SingleWAL write
// switch is off (rolling-restart compatibility).
func (o Options) Validate() error {
	if o.IORingSQPoll && !o.IORing {
		return fmt.Errorf("filestore: IORingSQPoll requires IORing")
	}
	if o.Mode != "" && o.Mode != ModeUnified && o.Mode != ModeRef {
		return fmt.Errorf("filestore: unknown Mode %q", o.Mode)
	}
	return nil
}

// resolvedMode maps the switch pair to the effective new-file layout.
func (o Options) resolvedMode() Mode {
	if o.Mode != "" {
		return o.Mode
	}
	if o.SingleWAL {
		return ModeUnified
	}
	return ModeLegacy
}

// ---------------------------------------------------------------------------
// On-disk formats (little-endian).
//
// v2 file header (512 B, written once at creation):
//   [0:4]   magic "VWAL"
//   [4:6]   u16 formatMajor = 2
//   [6:8]   u16 fileFlags (bit0 singleWAL; bit1 batch-pad512)
//   [8:512] reserved zero
//
// v2 record (records start at offset 512):
//   [u32 totalLen]                     // = 24 + raftLen + dataLen + 4(crc)
//   [u16 ver=2][u16 flags]
//   [u64 base][u64 lsn]
//   [u32 raftLen][msgpack(Log without Data)]
//   [payload]
//   [u32 crc32c over bytes [4 .. 4+totalLen-4)]
//
// Batch: records concatenated, then zero padding to the next 512 B boundary.
// Padding is never part of any record; reload treats a zero u32 at a record
// boundary as "skip to next 512 B boundary once", and a second consecutive
// zero as end-of-data (fallocate'd regions read as zeros).
//
// v1 (prototype compat, read-only): [u32 totalLen][u16 ver=1][u16 flags]
//   [u64 base][u64 lsn][u32 raftLen][msgpack(Log without Data)][data] — no CRC.
//
// legacy: [u32 payloadLen][msgpack(Log)] repeated.
const (
	fileMagic     = "VWAL"
	formatV2      = 2
	fileHeaderLen = 512

	recHeaderV2 = 4 + 2 + 2 + 8 + 8 + 4 // totalLen+ver+flags+base+lsn+raftLen = 28
	recTrailV2  = 4                     // crc32

	fileFlagSingleWAL = 1 << 0
	fileFlagPad512    = 1 << 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type lsnPos struct {
	index uint64
	off   int64
}

// redoPos is an entry of the ref-mode redo presence map (in-memory only;
// rebuilt by scanning redo.log on reload).
type redoPos struct {
	off    int64
	length int
}

// errScanStop is the internal sentinel for torn/corrupt tails in scanRecords.
var errScanStop = errors.New("filestore: torn tail")

// scanRecords walks [u32 len][body] records starting at start. When
// padBatches (v2 formats), a zero length at a record boundary is batch-tail
// padding: skip to the next 512B boundary once (off advances even on break so
// endOff stays batch-aligned); a second consecutive zero = end of data.
// yield returns errScanStop to end the scan at a torn/corrupt record.
// Returns the end of good data (the next append position).
func scanRecords(raw []byte, start int64, padBatches bool, yield func(off int64, body []byte) error) (int64, error) {
	buf := bytes.NewReader(raw[start:])
	off := start
	zeroRun := 0
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(buf, lenBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return off, err
		}
		n := int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if n <= 0 {
			zeroRun++
			if padBatches && zeroRun == 1 {
				next := align512(off)
				off = next
				if next >= int64(len(raw)) {
					break
				}
				if _, err := buf.Seek(next-start, io.SeekStart); err != nil {
					return off, err
				}
				continue
			}
			break
		}
		zeroRun = 0
		body := make([]byte, n)
		if _, err := io.ReadFull(buf, body); err != nil {
			break // crash-truncated tail
		}
		if err := yield(off, body); err != nil {
			if err == errScanStop {
				break
			}
			return off, err
		}
		off += 4 + n
	}
	return off, nil
}

// Store is an append-only file-backed raft.LogStore.
type Store struct {
	mu      sync.Mutex
	path    string
	f       *os.File // raft.log (ref mode: meta records)
	opts    Options
	first   uint64 // index of logs[0] (0 when empty)
	logs    []*raft.Log
	offsets []int64 // file offset of logs[i] (points at the record's len field)
	endOff  int64   // authoritative append position (512-aligned when v2)

	format   string // "v2-ref" | "v2-singlewal" | "v1-singlewal" | "legacy" — detected
	directIO bool   // effective after probe (v2 formats only)

	// ref mode only: redo.log is the data file; raft.log holds pointer meta.
	redoF    *os.File
	redoEnd  int64              // redo.log good-data end (append position)
	redoMap  map[uint64]redoPos // lsn → presence (data authority for reads)
	refSegs  [][]uint64         // parallel to logs: segment LSNs of entry i (nil = inline)
	refBases []uint64           // parallel to logs: base of entry i

	lsnIndex map[uint64]lsnPos // sparse index (only when SparseIndex)
	ioringOK uint64            // io_uring chains actually completed (probe evidence)

	unboundBytes int64 // P3: direct-written bytes not yet bound by raft meta
}

var _ raft.LogStore = (*Store)(nil)

// Open opens (or creates) the log store in dir and reloads its contents.
// The on-disk format is auto-detected; opts.SingleWAL/Mode only affect files
// created by this call. In ref mode dir also holds redo.log (the data file).
func Open(dir string, opts Options) (*Store, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{path: path, f: f, opts: opts}

	metaInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	metaNew := metaInfo.Size() == 0

	// Resolve the layout: existing files keep their own format (detected in
	// reload); new files follow the switches.
	wantRef := opts.resolvedMode() == ModeRef
	if wantRef || !metaNew {
		// ref pairing checks happen once the format is known; open redo.log now
		// if it exists or ref is requested.
		redoPath := filepath.Join(dir, redoFileName)
		if _, err := os.Stat(redoPath); err == nil || wantRef {
			rf, err := os.OpenFile(redoPath, os.O_CREATE|os.O_RDWR, 0o644)
			if err != nil {
				_ = f.Close()
				return nil, err
			}
			ri, err := rf.Stat()
			if err != nil {
				_ = rf.Close()
				_ = f.Close()
				return nil, err
			}
			if metaNew && ri.Size() > 0 {
				_ = rf.Close()
				_ = f.Close()
				return nil, fmt.Errorf("filestore: %s exists (%d B) without its meta file — refusing to guess", redoPath, ri.Size())
			}
			s.redoF = rf
		}
	}

	if err := s.reload(); err != nil {
		_ = f.Close()
		if s.redoF != nil {
			_ = s.redoF.Close()
		}
		return nil, err
	}

	// O_DIRECT tier: only for v2 formats (batch-tail padding guarantees
	// offset/length alignment); probed per file, falls back to buffered.
	if opts.DirectIO && (s.format == "v2-singlewal" || s.format == "v2-ref") {
		if df, ok := reprobeDirect(path, f, opts.Logger); ok {
			s.f = df
			s.directIO = true
			_ = f.Close()
		}
		if s.format == "v2-ref" && s.redoF != nil {
			if df, ok := reprobeDirect(filepath.Join(dir, redoFileName), s.redoF, opts.Logger); ok {
				_ = s.redoF.Close()
				s.redoF = df
			} else if !s.directIO {
				// keep consistent: both files buffered
			} else if opts.Logger != nil {
				opts.Logger.Printf("[WARN] filestore: redo.log O_DIRECT probe failed, redo writes stay buffered")
			}
		}
	}
	if opts.Logger != nil {
		opts.Logger.Printf("[INFO] filestore: %s format=%s directIO=%v first=%d last=%d endOff=%d",
			path, s.format, s.directIO, s.first, s.lastIndexLocked(), s.endOff)
	}
	return s, nil
}

// reprobeDirect reopens path with O_DIRECT and probes an aligned pwrite.
// On any failure the store stays buffered (探测回退).
func reprobeDirect(path string, cur *os.File, logger *log.Logger) (*os.File, bool) {
	info, err := cur.Stat()
	if err != nil {
		return nil, false
	}
	df, err := os.OpenFile(path, os.O_RDWR|unix.O_DIRECT, 0o644)
	if err != nil {
		if logger != nil {
			logger.Printf("[WARN] filestore: O_DIRECT open %s: %v (buffered fallback)", path, err)
		}
		return nil, false
	}
	buf := alignedBytes(512)
	probeAt := int64(1 << 20) // beyond any real data; truncated back afterwards
	if _, err := df.WriteAt(buf, probeAt); err != nil {
		_ = df.Close()
		if logger != nil {
			logger.Printf("[WARN] filestore: O_DIRECT probe write %s: %v (buffered fallback)", path, err)
		}
		return nil, false
	}
	_ = df.Truncate(info.Size())
	return df, true
}

// Format returns the detected on-disk format ("v2-singlewal" / "v1-singlewal" /
// "legacy") — the file's own format, independent of the current switches.
func (s *Store) Format() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.format
}

// IsMonotonic implements raft.MonotonicLogStore: filestore 是位置式追加存储
// (first + 连续索引算术), 无法表示快照安装留下的低位空洞 —— 因此快照安装时
// 走 removeOldLogs 全量删除(而非保留尾部的 compactLogs), 让 first 归零重铺,
// 也保证 StoreLogs 连续性护栏永不误伤合法的快照重接。
func (s *Store) IsMonotonic() bool { return true }

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		return err
	}
	if s.redoF != nil {
		if err := s.redoF.Sync(); err != nil {
			return err
		}
		if err := s.redoF.Close(); err != nil {
			return err
		}
	}
	return s.f.Close()
}

func (s *Store) lastIndexLocked() uint64 {
	if len(s.logs) == 0 {
		return 0
	}
	return s.logs[len(s.logs)-1].Index
}

// ---------------------------------------------------------------------------
// Encoding.

func encodeLogMsgpack(l *raft.Log) ([]byte, error) {
	var buf []byte
	if err := codec.NewEncoderBytes(&buf, &msgpackHandle).Encode(l); err != nil {
		return nil, err
	}
	return buf, nil
}

// encodeV2 builds one v2 record. The codec splits Data into envelope
// (base/lsn, stored in native header fields) and payload (stored after the
// raft msgpack header), so the same bytes serve the raft view (GetLog) and
// the redo view (LookupByLSN / raw scan) without duplication.
func (s *Store) encodeV2(l *raft.Log) (rec []byte, base, lsn uint64, err error) {
	payload := l.Data
	if s.opts.Codec != nil {
		base, lsn, payload = s.opts.Codec.Split(l)
	}
	tmp := *l
	tmp.Data = nil
	raftBuf, err := encodeLogMsgpack(&tmp)
	if err != nil {
		return nil, 0, 0, err
	}
	totalLen := (recHeaderV2 - 4) + len(raftBuf) + len(payload) + recTrailV2
	rec = make([]byte, 4+totalLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totalLen))
	binary.LittleEndian.PutUint16(rec[4:6], formatV2)
	binary.LittleEndian.PutUint16(rec[6:8], 0)
	binary.LittleEndian.PutUint64(rec[8:16], base)
	binary.LittleEndian.PutUint64(rec[16:24], lsn)
	binary.LittleEndian.PutUint32(rec[24:28], uint32(len(raftBuf)))
	copy(rec[28:], raftBuf)
	copy(rec[28+len(raftBuf):], payload)
	crc := crc32.Checksum(rec[4:len(rec)-4], castagnoli)
	binary.LittleEndian.PutUint32(rec[len(rec)-4:], crc)
	return rec, base, lsn, nil
}

// encodeV1 builds a prototype v1 record (no CRC, no padding) — used only to
// keep appending to existing v1 files without format mixing.
func (s *Store) encodeV1(l *raft.Log) (rec []byte, lsn uint64, err error) {
	payload := l.Data
	var base uint64
	if s.opts.Codec != nil {
		base, lsn, payload = s.opts.Codec.Split(l)
	}
	tmp := *l
	tmp.Data = nil
	raftBuf, err := encodeLogMsgpack(&tmp)
	if err != nil {
		return nil, 0, err
	}
	totalLen := (recHeaderV2 - 4) + len(raftBuf) + len(payload)
	rec = make([]byte, 4+totalLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totalLen))
	binary.LittleEndian.PutUint16(rec[4:6], 1)
	binary.LittleEndian.PutUint64(rec[8:16], base)
	binary.LittleEndian.PutUint64(rec[16:24], lsn)
	binary.LittleEndian.PutUint32(rec[24:28], uint32(len(raftBuf)))
	copy(rec[28:], raftBuf)
	copy(rec[28+len(raftBuf):], payload)
	return rec, lsn, nil
}

// decodeV2 verifies and decodes one v2 record body (bytes after totalLen).
func (s *Store) decodeV2(body []byte) (*raft.Log, uint64, error) {
	if len(body) < recHeaderV2-4+recTrailV2 {
		return nil, 0, errors.New("short v2 record")
	}
	want := binary.LittleEndian.Uint32(body[len(body)-4:])
	got := crc32.Checksum(body[:len(body)-4], castagnoli)
	if want != got {
		return nil, 0, errors.New("crc mismatch")
	}
	raftLen := int(binary.LittleEndian.Uint32(body[20:24]))
	if raftLen < 0 || raftLen > len(body)-(recHeaderV2-4)-recTrailV2 {
		return nil, 0, errors.New("bad raftLen")
	}
	base := binary.LittleEndian.Uint64(body[4:12])
	lsn := binary.LittleEndian.Uint64(body[12:20])
	raftBody := body[recHeaderV2-4 : recHeaderV2-4+raftLen]
	payload := body[recHeaderV2-4+raftLen : len(body)-recTrailV2]
	l := &raft.Log{}
	if err := codec.NewDecoderBytes(raftBody, &msgpackHandle).Decode(l); err != nil {
		return nil, 0, err
	}
	if s.opts.Codec != nil {
		l.Data = s.opts.Codec.Merge(base, lsn, append([]byte(nil), payload...))
	} else {
		l.Data = append([]byte(nil), payload...)
	}
	return l, lsn, nil
}

// decodeV1 reads a prototype v1 record (no CRC) — read-only compatibility.
func decodeV1(body []byte) (*raft.Log, uint64, error) {
	if len(body) < recHeaderV2-4 {
		return nil, 0, errors.New("short v1 record")
	}
	raftLen := int(binary.LittleEndian.Uint32(body[20:24]))
	if raftLen < 0 || raftLen > len(body)-(recHeaderV2-4) {
		return nil, 0, errors.New("bad raftLen")
	}
	lsn := binary.LittleEndian.Uint64(body[12:20])
	l := &raft.Log{}
	if err := codec.NewDecoderBytes(body[recHeaderV2-4:recHeaderV2-4+raftLen], &msgpackHandle).Decode(l); err != nil {
		return nil, 0, err
	}
	l.Data = append([]byte(nil), body[recHeaderV2-4+raftLen:]...)
	return l, lsn, nil
}

func align512(off int64) int64 { return (off + 511) &^ 511 }

// ---------------------------------------------------------------------------
// Reload with format auto-detection and torn-tail handling.
//
// Detection order: v2 magic (+ref flag) → v1 sniff (ver field) → legacy. The
// detected format governs both reading AND subsequent appends: a legacy file
// keeps receiving legacy records even when SingleWAL is on (探测回退/在线兼容),
// so an operator can flip the switch without corrupting existing PVCs.
// Ref mode reads redo.log (data authority) first, then the meta file, and
// drops meta entries whose redo bytes were torn away (12.14.3 ③).
func (s *Store) reload() error {
	if err := s.f.Sync(); err != nil {
		return err
	}
	info, err := s.f.Stat()
	if err != nil {
		return err
	}
	raw, err := s.readWhole(s.f, info.Size())
	if err != nil {
		return err
	}

	mode := s.opts.resolvedMode()
	if len(raw) == 0 {
		// New file: pick format from the switches and lay down the header(s).
		switch mode {
		case ModeUnified, ModeRef:
			s.format = map[Mode]string{ModeUnified: "v2-singlewal", ModeRef: "v2-ref"}[mode]
			flags := uint16(fileFlagSingleWAL | fileFlagPad512)
			if mode == ModeRef {
				flags |= fileFlagRef
			}
			if err := writeFileHeader(s.f, flags); err != nil {
				return err
			}
			if mode == ModeRef {
				if s.redoF == nil {
					return fmt.Errorf("filestore: ref mode requires redo.log")
				}
				if err := writeFileHeader(s.redoF, fileFlagSingleWAL|fileFlagPad512|fileFlagRef|fileFlagData); err != nil {
					return err
				}
				s.redoEnd = fileHeaderLen
			}
			s.endOff = fileHeaderLen
			_, err := s.f.Seek(fileHeaderLen, io.SeekStart)
			return err
		default:
			s.format = "legacy"
			return nil
		}
	}

	// Existing file: detect.
	start := int64(0)
	s.format = "legacy"
	switch {
	case len(raw) >= fileHeaderLen && string(raw[0:4]) == fileMagic &&
		binary.LittleEndian.Uint16(raw[4:6]) == formatV2:
		flags := binary.LittleEndian.Uint16(raw[6:8])
		if flags&fileFlagRef != 0 {
			s.format = "v2-ref"
		} else {
			s.format = "v2-singlewal"
		}
		start = fileHeaderLen
	case len(raw) >= 6 && binary.LittleEndian.Uint16(raw[4:6]) == 1:
		// v1 prototype: [u32 len][u16 ver=1]... (legacy msgpack never starts
		// with bytes 0x01 0x00 at [4:6] — its body starts with a fixmap).
		s.format = "v1-singlewal"
	}
	if s.format != "legacy" && s.opts.Logger != nil && mode == ModeLegacy {
		s.opts.Logger.Printf("[INFO] filestore: %s is %s, write switch off (format follows file)", s.path, s.format)
	}

	if s.format == "v2-ref" {
		if s.redoF == nil {
			return fmt.Errorf("filestore: %s is v2-ref but redo.log is missing", s.path)
		}
		ri, err := s.redoF.Stat()
		if err != nil {
			return err
		}
		rawRedo, err := s.readWhole(s.redoF, ri.Size())
		if err != nil {
			return err
		}
		if err := s.reloadRef(raw, rawRedo); err != nil {
			return err
		}
		if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
			return err
		}
		if _, err := s.redoF.Seek(s.redoEnd, io.SeekStart); err != nil {
			return err
		}
		return nil
	}

	padBatches := s.format == "v2-singlewal"
	goodEnd, err := scanRecords(raw, start, padBatches, func(off int64, body []byte) error {
		var l *raft.Log
		var lsn uint64
		var derr error
		switch s.format {
		case "v2-singlewal":
			l, lsn, derr = s.decodeV2(body)
		case "v1-singlewal":
			l, lsn, derr = decodeV1(body)
		default:
			ll := &raft.Log{}
			derr = codec.NewDecoderBytes(body, &msgpackHandle).Decode(ll)
			l = ll
		}
		if derr != nil {
			return errScanStop // corrupt/torn tail — truncation point
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, off)
		if s.opts.SparseIndex && lsn != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsn] = lsnPos{index: l.Index, off: off}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.endOff = goodEnd
	s.truncateAtGapLocked()
	// Park the cursor at the end of good data; reload reads via ReadAt and
	// never moves it. A torn tail is simply overwritten by the next append.
	if _, err := s.f.Seek(goodEnd, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// truncateAtGapLocked 在重载后发现索引空洞时, 从第一个洞处截断内存视图并把
// meta 文件 ftruncate(+v2 补零对齐), 保证 LastIndex/GetLog 一致。洞以上的后缀
// 对 raft 不可达(processLogs 无法跨过洞 apply), 截断后由领导者按 prevLog 重铺。
// 返回是否发生了截断。调用方须持有 s.mu(或处于 Open 单线程期)。
func (s *Store) truncateAtGapLocked() bool {
	for i := 1; i < len(s.logs); i++ {
		if s.logs[i].Index == s.logs[i-1].Index+1 {
			continue
		}
		cutOff := s.offsets[i]
		if s.opts.Logger != nil {
			s.opts.Logger.Printf("[WARN] filestore: %s 索引空洞 %d→%d —— 重载截断丢弃其上的 %d 条(等领导者重铺)",
				s.path, s.logs[i-1].Index, s.logs[i].Index, len(s.logs)-i)
		}
		s.logs = s.logs[:i]
		s.offsets = s.offsets[:i]
		if s.format == "v2-ref" {
			s.refSegs = s.refSegs[:i]
			s.refBases = s.refBases[:i]
		}
		if err := s.f.Truncate(cutOff); err == nil {
			if s.format == "v2-singlewal" || s.format == "v2-ref" {
				if padded := align512(cutOff); padded > cutOff {
					_, _ = s.f.WriteAt(make([]byte, padded-cutOff), cutOff)
					cutOff = padded
				}
			}
			s.endOff = cutOff
		}
		if s.lsnIndex != nil && len(s.logs) > 0 {
			lastKept := s.logs[len(s.logs)-1].Index
			for lsn, pos := range s.lsnIndex {
				if pos.index > lastKept {
					delete(s.lsnIndex, lsn)
				}
			}
		}
		return true
	}
	return false
}

// writeFileHeader lays down the 512 B v2 header.
func writeFileHeader(f *os.File, flags uint16) error {
	hdr := make([]byte, fileHeaderLen)
	copy(hdr[0:4], fileMagic)
	binary.LittleEndian.PutUint16(hdr[4:6], formatV2)
	binary.LittleEndian.PutUint16(hdr[6:8], flags)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		return err
	}
	return f.Sync()
}

// readWhole reads size bytes from f at offset 0, honoring O_DIRECT alignment.
func (s *Store) readWhole(f *os.File, size int64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	if s.directIO {
		return readAtAligned(f, 0, int(size))
	}
	raw := make([]byte, size)
	if _, err := f.ReadAt(raw, 0); err != nil && err != io.EOF {
		return nil, err
	}
	return raw, nil
}

// ---------------------------------------------------------------------------
// StoreLogs: append a batch + one durability barrier (raft's durability
// contract is exactly "on disk when StoreLogs returns").
func (s *Store) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(logs) == 0 {
		return nil
	}
	// 连续性护栏: raft 契约要求追加严格连续(leader 顺序分配, follower 经
	// prevLog/冲突截断对齐)。历史上 v2-ref 在高并发+快照压实窗口出现过一个
	// 索引空洞(磁盘上 75304 直接跳到 75306)且一路静默, 直到 apply GetLog
	// panic 才暴露 —— 任何不连续都必须在落盘前炸响, 而不是埋成洞。
	if len(s.logs) > 0 {
		if want, got := s.lastIndexLocked()+1, logs[0].Index; got != want {
			return fmt.Errorf("filestore: non-contiguous append: store last=%d, batch first=%d (len=%d) — refusing to write a hole",
				want-1, got, len(logs))
		}
	}
	for i := 1; i < len(logs); i++ {
		if logs[i].Index != logs[i-1].Index+1 {
			return fmt.Errorf("filestore: non-contiguous batch: [%d]=%d after %d — refusing",
				i, logs[i].Index, logs[i-1].Index)
		}
	}
	if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
		return err
	}

	if s.format == "v2-ref" {
		return s.storeLogsRef(logs)
	}
	if s.format == "v2-singlewal" {
		return s.storeLogsV2(logs)
	}
	if s.format == "v1-singlewal" {
		return s.storeLogsV1(logs)
	}
	return s.storeLogsLegacy(logs)
}

// storeLogsV1 keeps appending v1-format records to a prototype-era file
// (no CRC, no batch padding). New v1 files are never created.
func (s *Store) storeLogsV1(logs []*raft.Log) error {
	lsns := make([]uint64, len(logs))
	encoded := make([][]byte, len(logs))
	for i, l := range logs {
		rec, lsn, err := s.encodeV1(l)
		if err != nil {
			return err
		}
		lsns[i] = lsn
		encoded[i] = rec
	}
	for i, l := range logs {
		if _, err := s.f.Write(encoded[i]); err != nil {
			return err
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, s.endOff)
		if s.opts.SparseIndex && lsns[i] != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsns[i]] = lsnPos{index: l.Index, off: s.endOff}
		}
		s.endOff += int64(len(encoded[i]))
	}
	return s.barrier()
}

// storeLogsV2 encodes the batch as v2 records, concatenates, pads the batch
// tail to 512 B, and persists with one write + one barrier (or one io_uring
// enter). Data bytes land exactly once — this is the 12.14 single-copy point.
func (s *Store) storeLogsV2(logs []*raft.Log) error {
	lsns := make([]uint64, len(logs))
	var batch bytes.Buffer
	for i, l := range logs {
		rec, _, lsn, err := s.encodeV2(l)
		if err != nil {
			return err
		}
		lsns[i] = lsn
		batch.Write(rec)
	}
	if pad := align512(int64(batch.Len())) - int64(batch.Len()); pad > 0 {
		batch.Write(make([]byte, pad))
	}
	buf := batch.Bytes()
	if s.directIO {
		buf = alignedCopy(buf)
	}
	baseOff := s.endOff

	if s.opts.IORing {
		if err := storeLogsIORingChain(s.f, baseOff, [][]byte{buf}, s.opts.IORingSQPoll); err == nil {
			s.ioringOK++
			s.commitBatch(logs, lsns, baseOff, buf)
			return nil
		}
		// fall through to classic write + barrier on any error
	}
	if _, err := s.f.Write(buf); err != nil {
		return err
	}
	if err := s.barrier(); err != nil {
		return err
	}
	s.commitBatch(logs, lsns, baseOff, buf)
	return nil
}

// commitBatch advances the in-memory view after a durable batch write.
// recordOffsets walks the concatenated buffer to keep per-record offsets
// (needed by DeleteRange at record granularity).
func (s *Store) commitBatch(logs []*raft.Log, lsns []uint64, baseOff int64, buf []byte) {
	recOff := int64(0)
	for i, l := range logs {
		if s.first == 0 {
			s.first = l.Index
		}
		recLen := int64(binary.LittleEndian.Uint32(buf[recOff : recOff+4])) + 4
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, baseOff+recOff)
		if s.opts.SparseIndex && lsns[i] != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsns[i]] = lsnPos{index: l.Index, off: baseOff + recOff}
		}
		recOff += recLen
	}
	s.endOff = baseOff + align512(recOff)
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
	}
	if s.opts.UseSyncFileRange {
		_ = syncFileRange(s.f, 0, 0)
	}
}

// storeLogsLegacy is the upstream-compatible path: [u32][msgpack] per record.
func (s *Store) storeLogsLegacy(logs []*raft.Log) error {
	encoded := make([][]byte, len(logs))
	for i, l := range logs {
		body, err := encodeLogMsgpack(l)
		if err != nil {
			return err
		}
		rec := make([]byte, 4+len(body))
		binary.LittleEndian.PutUint32(rec[:4], uint32(len(body)))
		copy(rec[4:], body)
		encoded[i] = rec
	}
	baseOff := s.endOff
	if s.opts.IORing {
		if err := storeLogsIORingChain(s.f, baseOff, encoded, s.opts.IORingSQPoll); err == nil {
			s.ioringOK++
			for i, l := range logs {
				if s.first == 0 {
					s.first = l.Index
				}
				s.logs = append(s.logs, l)
				s.offsets = append(s.offsets, baseOff)
				baseOff += int64(len(encoded[i]))
			}
			s.endOff = baseOff
			if s.opts.PreallocateSegments {
				_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
			}
			if s.opts.UseSyncFileRange {
				_ = syncFileRange(s.f, 0, 0)
			}
			return nil
		}
	}
	for i, l := range logs {
		if _, err := s.f.Write(encoded[i]); err != nil {
			return err
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, s.endOff)
		s.endOff += int64(len(encoded[i]))
	}
	if err := s.barrier(); err != nil {
		return err
	}
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
	}
	if s.opts.UseSyncFileRange {
		_ = syncFileRange(s.f, 0, 0)
	}
	return nil
}

// barrier is the durability barrier on the main (meta/unified) file.
func (s *Store) barrier() error { return s.barrierOn(s.f) }

// barrierOn: io_uring FSYNC → fdatasync → fsync, per fd.
func (s *Store) barrierOn(f *os.File) error {
	if s.opts.IORing {
		if err := ioringFsync(f, s.opts.IORingSQPoll); err == nil {
			s.ioringOK++
			return nil
		}
	}
	if s.opts.Fdatasync {
		return fdatasync(f)
	}
	return f.Sync()
}

func (s *Store) StoreLog(l *raft.Log) error { return s.StoreLogs([]*raft.Log{l}) }

func (s *Store) GetLog(index uint64, l *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == 0 || index < s.first {
		return raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		return raft.ErrLogNotFound
	}
	if got := s.logs[off].Index; got != index {
		// 位置算术命中了别的条目 —— 说明中间有洞(不连续追加)。绝不可把
		// 错误条目静默交给调用方(raft 会拿它做 prevLog 校验/apply)。
		return fmt.Errorf("filestore: hole detected: GetLog(%d) maps to entry %d (first=%d len=%d): %w",
			index, got, s.first, len(s.logs), raft.ErrLogNotFound)
	}
	*l = *s.logs[off]
	// ref mode: redo entries store only the pointer; reconstruct Data from
	// redo.log (raft replication and FSM apply both read through here).
	if s.format == "v2-ref" && off < uint64(len(s.refSegs)) && len(s.refSegs[off]) > 0 {
		data, err := s.mergeEntry(s.refBases[off], s.refSegs[off])
		if err != nil {
			return err
		}
		l.Data = data
	}
	return nil
}

func (s *Store) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first, nil
}

func (s *Store) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndexLocked(), nil
}

// DeleteRange 删除闭区间 [min, max] 的条目, 区间外的条目必须存活(raft 契约:
// 快照压实删前缀 [first, snapIdx-trailing], 冲突截断删后缀 [conflict, last])。
//
// 实现分两路:
//   - 后缀删除(min > first): 文件直接截断到 min 的记录偏移, 廉价。
//   - 前缀/整段删除(min <= first): 追加式文件无法原地挖洞 —— 内存丢弃 +
//     幸存者记录原字节打包重写(tmp+rename+fsync dir, 与 CompactRedo 同模式)。
//     历史上这里错误地复用了截断路径: keep=min-first=0 → Truncate(offsets[0])
//     → 把整个日志(包括压实必须保留的后缀)全部抹掉, 只能靠 InstallSnapshot
//     自愈, 且在 apply 与压实交叠时直接 panic("log not found")。
//
// Ref 模式: 只动 meta 文件 —— LSN 不随 index 单调(批量提交折叠不连续段),
// redo.log 字节绝不中段截断; 孤儿 redo 空间由 CompactRedo 在检查点回收。
// v2 格式截断/重写后补零到 512B 边界, 使下一批次保持 512 对齐(O_DIRECT 安全)。
func (s *Store) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) == 0 || min > max || max < s.first {
		return nil
	}
	last := s.lastIndexLocked()

	// 后缀删除(冲突截断): 现有廉价截断路径。
	if min > s.first {
		keep := int(min - s.first)
		if keep > len(s.logs) {
			keep = len(s.logs)
		}
		// raft 语义上 max==last; 若调用方给了更小的 max(中段删除 —— raft 的
		// 两个调用点(压实/冲突)都不会这样用), 防御性回落到打包重写, 绝不误删
		// (max, last] 的存活条目。
		if max < last {
			return s.deleteRangeRewriteLocked(min, max)
		}
		truncAt := s.offsets[keep]
		s.logs = append([]*raft.Log(nil), s.logs[:keep]...)
		s.offsets = append([]int64(nil), s.offsets[:keep]...)
		if s.format == "v2-ref" {
			s.refSegs = append([][]uint64(nil), s.refSegs[:keep]...)
			s.refBases = append([]uint64(nil), s.refBases[:keep]...)
		}
		if len(s.logs) == 0 {
			s.first = 0
		}
		if s.lsnIndex != nil {
			for lsn, pos := range s.lsnIndex {
				if pos.index >= min {
					delete(s.lsnIndex, lsn)
				}
			}
		}
		oldEnd := s.endOff
		if err := s.f.Truncate(truncAt); err != nil {
			return err
		}
		s.endOff = truncAt
		if s.format == "v2-singlewal" || s.format == "v2-ref" {
			if padded := align512(truncAt); padded > truncAt {
				if _, err := s.f.WriteAt(make([]byte, padded-truncAt), truncAt); err != nil {
					return err
				}
				s.endOff = padded
			}
		}
		if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
			return err
		}
		if err := s.barrier(); err != nil {
			return err
		}
		if s.opts.UseFadvise && oldEnd > s.endOff {
			_ = fadviseDontNeed(s.f, s.endOff, oldEnd-s.endOff)
		}
		return nil
	}

	// 前缀/整段删除(min <= first): 幸存者 = (max, last]。
	return s.deleteRangeRewriteLocked(min, max)
}

// deleteRangeRewriteLocked 处理所有 min <= s.first 的删除: 把 [max,last] 外的
// 幸存者记录原字节打包重写为新 meta 文件并原子 rename。调用方须持有 s.mu。
func (s *Store) deleteRangeRewriteLocked(min, max uint64) error {
	last := s.lastIndexLocked()
	if max >= last {
		// 整段删除(RecoverCluster): 截回起始偏移即可。
		truncAt := int64(0)
		if s.format == "v2-singlewal" || s.format == "v2-ref" || s.format == "v1-singlewal" {
			truncAt = s.offsets[0] // v2=header 之后; v1/legacy=0
		}
		s.logs = nil
		s.offsets = nil
		if s.format == "v2-ref" {
			s.refSegs = nil
			s.refBases = nil
		}
		s.first = 0
		s.lsnIndex = nil
		if err := s.f.Truncate(truncAt); err != nil {
			return err
		}
		s.endOff = truncAt
		if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
			return err
		}
		return s.barrier()
	}

	drop := int(max - s.first + 1) // 删除 [first, max], 保留 s.logs[drop:]
	if drop <= 0 || drop >= len(s.logs) {
		return nil
	}
	isV2 := s.format == "v2-singlewal" || s.format == "v2-ref"

	dir := filepath.Dir(s.path)
	tmpPath := s.path + ".compact.tmp"
	nf, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	writeOff := int64(0)
	if isV2 {
		hdr := make([]byte, fileHeaderLen)
		if _, err := s.f.ReadAt(hdr, 0); err != nil { // 原样保留头部(flags)
			nf.Close()
			return err
		}
		if _, err := nf.WriteAt(hdr, 0); err != nil {
			nf.Close()
			return err
		}
		writeOff = fileHeaderLen
	}
	var batch bytes.Buffer
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		if isV2 {
			if pad := align512(int64(batch.Len())) - int64(batch.Len()); pad > 0 {
				batch.Write(make([]byte, pad))
			}
		}
		buf := batch.Bytes()
		if s.directIO {
			buf = alignedCopy(buf)
		}
		if _, err := nf.WriteAt(buf, writeOff); err != nil {
			return err
		}
		writeOff += int64(batch.Len())
		batch.Reset()
		return nil
	}
	newOffsets := make([]int64, 0, len(s.logs)-drop)
	for i := drop; i < len(s.logs); i++ {
		var lenBuf [4]byte
		if _, err := s.f.ReadAt(lenBuf[:], s.offsets[i]); err != nil {
			nf.Close()
			return err
		}
		recLen := 4 + int(binary.LittleEndian.Uint32(lenBuf[:]))
		raw := make([]byte, recLen)
		if _, err := s.f.ReadAt(raw, s.offsets[i]); err != nil {
			nf.Close()
			return err
		}
		newOffsets = append(newOffsets, writeOff+int64(batch.Len()))
		batch.Write(raw)
		if batch.Len() >= 1<<20 {
			if err := flush(); err != nil {
				nf.Close()
				return err
			}
		}
	}
	if err := flush(); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Close(); err != nil {
		return err
	}
	// rename 成功前绝不关旧句柄(失败则旧文件仍是权威)。
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	syncDir(dir)
	newF, err := os.OpenFile(s.path, os.O_RDWR|openDirectFlag(s.directIO), 0o644)
	if err != nil {
		return err
	}
	_ = s.f.Close()
	s.f = newF
	s.logs = append([]*raft.Log(nil), s.logs[drop:]...)
	s.offsets = newOffsets
	if s.format == "v2-ref" {
		s.refSegs = append([][]uint64(nil), s.refSegs[drop:]...)
		s.refBases = append([]uint64(nil), s.refBases[drop:]...)
	}
	s.first = s.logs[0].Index
	s.endOff = writeOff
	if s.lsnIndex != nil {
		for lsn, pos := range s.lsnIndex {
			if pos.index >= min && pos.index <= max {
				delete(s.lsnIndex, lsn)
			}
		}
	}
	if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
		return err
	}
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
	}
	return nil
}

// GetLogRaw returns the raw Data bytes for index (12.8.2 raw-bytes path).
func (s *Store) GetLogRaw(index uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == 0 || index < s.first {
		return nil, raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		return nil, raft.ErrLogNotFound
	}
	if s.format == "v2-ref" && off < uint64(len(s.refSegs)) && len(s.refSegs[off]) > 0 {
		data, err := s.mergeEntry(s.refBases[off], s.refSegs[off])
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return append([]byte(nil), s.logs[off].Data...), nil
}

// LookupByLSN resolves a redo LSN to its raft index via the sparse index.
// The index is populated when: unified/v1 + SparseIndex switch, or ref mode
// (unconditionally — it is the read path).
func (s *Store) LookupByLSN(lsn uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lsnIndex == nil {
		return 0, false
	}
	pos, ok := s.lsnIndex[lsn]
	if !ok {
		return 0, false
	}
	return pos.index, true
}

// Stats is an observability snapshot for /storeinfo-style endpoints.
type Stats struct {
	Format        string `json:"format"`
	SparseIndex   bool   `json:"sparseIndex"`
	LSNEntries    int    `json:"lsnEntries"`
	FirstIndex    uint64 `json:"firstIndex"`
	LastIndex     uint64 `json:"lastIndex"`
	EndOffset     int64  `json:"endOffset"`
	RedoEndOffset int64  `json:"redoEndOffset,omitempty"`
	IORing        bool   `json:"ioRing"`
	IORingOK      uint64 `json:"ioRingOK"` // chains actually completed (probe evidence)
	DirectIO      bool   `json:"directIO"`
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Format:        s.format,
		SparseIndex:   s.opts.SparseIndex,
		LSNEntries:    len(s.lsnIndex),
		FirstIndex:    s.first,
		LastIndex:     s.lastIndexLocked(),
		EndOffset:     s.endOff,
		RedoEndOffset: s.redoEnd,
		IORing:        s.opts.IORing,
		IORingOK:      s.ioringOK,
		DirectIO:      s.directIO,
	}
}

// ---------------------------------------------------------------------------
// Ref-mode business read API (12.14.3): the redo view of the store.

// LookupRedo resolves an lsn to its redo.log offset/length (presence map).
func (s *Store) LookupRedo(lsn uint64) (off int64, length int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos, ok := s.redoMap[lsn]
	return pos.off, pos.length, ok
}

// ReadRedo returns the redo payload for an lsn (unframed, pre-Merge).
func (s *Store) ReadRedo(lsn uint64) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, err := s.readRedoPayload(lsn)
	if err != nil {
		return nil, false
	}
	return payload, true
}

// ReadRedoRaw returns the raw framed record bytes (u32 length prefix +
// framer body) for an lsn — what a business reader ships downstream.
func (s *Store) ReadRedoRaw(lsn uint64) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos, ok := s.redoMap[lsn]
	if !ok {
		return nil, false
	}
	if s.directIO {
		raw, err := readAtAligned(s.redoF, pos.off, pos.length)
		if err != nil {
			return nil, false
		}
		return raw[:pos.length], true
	}
	raw := make([]byte, pos.length)
	if _, err := s.redoF.ReadAt(raw, pos.off); err != nil {
		return nil, false
	}
	return raw, true
}

// ---------------------------------------------------------------------------
// 12.9 helpers (best-effort, probed with fallback).

func fdatasync(f *os.File) error {
	if err := unix.Fdatasync(int(f.Fd())); err != nil {
		return f.Sync()
	}
	return nil
}

func preallocate(f *os.File, off, segSize int64) error {
	sz := segSize
	if sz <= 0 {
		sz = 64 << 20
	}
	target := ((off / sz) + 1) * sz
	n := target - off
	if n <= 0 {
		return nil
	}
	return unix.Fallocate(int(f.Fd()), 0, off, n)
}

func syncFileRange(f *os.File, off, n int64) error {
	return unix.SyncFileRange(int(f.Fd()), off, n, unix.SYNC_FILE_RANGE_WRITE)
}

func fadviseDontNeed(f *os.File, off, n int64) error {
	return unix.Fadvise(int(f.Fd()), off, n, unix.FADV_DONTNEED)
}
