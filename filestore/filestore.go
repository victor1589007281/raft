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

// ---------------------------------------------------------------------------
// Options: every knob defaults to off (zero value) — a zero Options is the
// upstream-equivalent legacy store.
type Options struct {
	// 12.7/12.14 single WAL.
	SingleWAL   bool      // write v2 unified format on NEW files
	SparseIndex bool      // build LSN→(index,off) index (requires SingleWAL)
	Codec       RedoCodec // nil = no redo envelope semantics

	// 12.9 kernel I/O knobs (probed with fallback).
	Fdatasync           bool  // Sync → Fdatasync (skip inode mtime flush)
	PreallocateSegments bool  // fallocate next segment eagerly
	SegmentSize         int64 // 0 = 64 MiB
	UseFadvise          bool  // DONTNEED truncated tail
	UseSyncFileRange    bool  // async writeback kick mid-batch
	IORing              bool  // io_uring WRITE→FSYNC single-enter chain
	IORingSQPoll        bool  // requires IORing

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
	return nil
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

// Store is an append-only file-backed raft.LogStore.
type Store struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	opts    Options
	first   uint64 // index of logs[0] (0 when empty)
	logs    []*raft.Log
	offsets []int64 // file offset of logs[i] (points at the record's len field)
	endOff  int64   // authoritative append position (512-aligned when v2)

	format string // "v2-singlewal" | "v1-singlewal" | "legacy" — detected

	lsnIndex map[uint64]lsnPos // sparse index (only when SparseIndex)
}

var _ raft.LogStore = (*Store)(nil)

// Open opens (or creates) the log store in dir and reloads its contents.
// The on-disk format is auto-detected; opts.SingleWAL only affects files
// created by this call.
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
	if err := s.reload(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if opts.Logger != nil {
		opts.Logger.Printf("[INFO] filestore: %s format=%s first=%d last=%d endOff=%d",
			path, s.format, s.first, s.lastIndexLocked(), s.endOff)
	}
	return s, nil
}

// Format returns the detected on-disk format ("v2-singlewal" / "v1-singlewal" /
// "legacy") — the file's own format, independent of the current switches.
func (s *Store) Format() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.format
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		return err
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
// Detection order: v2 magic → v1 sniff (ver field) → legacy. The detected
// format governs both reading AND subsequent appends: a legacy file keeps
// receiving legacy records even when SingleWAL is on (探测回退/在线兼容),
// so an operator can flip the switch without corrupting existing PVCs.
func (s *Store) reload() error {
	if err := s.f.Sync(); err != nil {
		return err
	}
	info, err := s.f.Stat()
	if err != nil {
		return err
	}
	raw := make([]byte, info.Size())
	if _, err := s.f.ReadAt(raw, 0); err != nil && err != io.EOF {
		return err
	}

	start := int64(0)
	s.format = "legacy"
	if len(raw) == 0 {
		// New file: pick format from the switch and lay down the v2 header.
		if s.opts.SingleWAL {
			s.format = "v2-singlewal"
			hdr := make([]byte, fileHeaderLen)
			copy(hdr[0:4], fileMagic)
			binary.LittleEndian.PutUint16(hdr[4:6], formatV2)
			binary.LittleEndian.PutUint16(hdr[6:8], fileFlagSingleWAL|fileFlagPad512)
			if _, err := s.f.WriteAt(hdr, 0); err != nil {
				return err
			}
			if err := s.f.Sync(); err != nil {
				return err
			}
			s.endOff = fileHeaderLen
			_, err := s.f.Seek(fileHeaderLen, io.SeekStart)
			return err
		}
	} else if len(raw) >= fileHeaderLen && string(raw[0:4]) == fileMagic &&
		binary.LittleEndian.Uint16(raw[4:6]) == formatV2 {
		s.format = "v2-singlewal"
		start = fileHeaderLen
	} else if len(raw) >= 6 && binary.LittleEndian.Uint16(raw[4:6]) == 1 {
		// v1 prototype: [u32 len][u16 ver=1]... (legacy msgpack never starts
		// with bytes 0x01 0x00 at [4:6] — its body starts with a fixmap).
		s.format = "v1-singlewal"
	}
	if s.format != "legacy" && s.opts.Logger != nil && !s.opts.SingleWAL {
		s.opts.Logger.Printf("[INFO] filestore: %s is %s, ignoring SingleWAL=off (format follows file)", s.path, s.format)
	}

	buf := bytes.NewReader(raw[start:])
	off := start
	zeroRun := 0
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(buf, lenBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return err
		}
		n := int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if n <= 0 {
			zeroRun++
			if s.format == "v2-singlewal" && zeroRun == 1 {
				// Batch-tail padding: advance to the next 512 B boundary.
				// off must advance EVEN when we break (end-of-data), so that
				// endOff stays batch-aligned and the next append does not
				// start mid-padding (which would strand it after a skip).
				// NB: the reader already consumed the 4-byte length field,
				// so seek ABSOLUTELY (slice-relative), not relative to off.
				next := align512(off)
				off = next
				if next >= int64(len(raw)) {
					break
				}
				if _, err := buf.Seek(next-start, io.SeekStart); err != nil {
					return err
				}
				continue
			}
			break // second consecutive zero run: fallocate'd tail / end of data
		}
		zeroRun = 0
		body := make([]byte, n)
		if _, err := io.ReadFull(buf, body); err != nil {
			break // crash-truncated tail
		}
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
			break // corrupt/torn tail — treat as truncation point
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
		off += 4 + n
	}
	s.endOff = off
	// Park the cursor at the end of good data; reload reads via ReadAt and
	// never moves it. A torn tail is simply overwritten by the next append.
	if _, err := s.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	return nil
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
	if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
		return err
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
	baseOff := s.endOff

	if s.opts.IORing {
		if err := storeLogsIORingChain(s.f, baseOff, [][]byte{buf}, s.opts.IORingSQPoll); err == nil {
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

// barrier is the durability barrier: io_uring FSYNC → fdatasync → fsync.
func (s *Store) barrier() error {
	if s.opts.IORing {
		if err := ioringFsync(s.f, s.opts.IORingSQPoll); err == nil {
			return nil
		}
	}
	if s.opts.Fdatasync {
		return fdatasync(s.f)
	}
	return s.f.Sync()
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
	*l = *s.logs[off]
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

// DeleteRange truncates the log at the first deleted index, both in memory
// and on disk, and prunes the sparse index. Entries at or above min are
// uncommitted by raft contract, so dropping them is redo-safe: the business
// re-ships the same LSN segment idempotently via the new leader.
func (s *Store) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) == 0 || min > max || max < s.first {
		return nil
	}
	keep := 0
	if min > s.first {
		keep = int(min - s.first)
		if keep > len(s.logs) {
			keep = len(s.logs)
		}
	}
	truncAt := s.offsets[keep]
	s.logs = append([]*raft.Log(nil), s.logs[:keep]...)
	s.offsets = append([]int64(nil), s.offsets[:keep]...)
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
	if _, err := s.f.Seek(truncAt, io.SeekStart); err != nil {
		return err
	}
	if err := s.barrier(); err != nil {
		return err
	}
	if s.opts.UseFadvise && oldEnd > truncAt {
		_ = fadviseDontNeed(s.f, truncAt, oldEnd-truncAt)
	}
	s.endOff = truncAt
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
	return append([]byte(nil), s.logs[off].Data...), nil
}

// LookupByLSN resolves a redo LSN to its raft index via the sparse index.
func (s *Store) LookupByLSN(lsn uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opts.SparseIndex || s.lsnIndex == nil {
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
	Format      string `json:"format"`
	SparseIndex bool   `json:"sparseIndex"`
	LSNEntries  int    `json:"lsnEntries"`
	FirstIndex  uint64 `json:"firstIndex"`
	LastIndex   uint64 `json:"lastIndex"`
	EndOffset   int64  `json:"endOffset"`
	IORing      bool   `json:"ioRing"`
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Format:      s.format,
		SparseIndex: s.opts.SparseIndex,
		LSNEntries:  len(s.lsnIndex),
		FirstIndex:  s.first,
		LastIndex:   s.lastIndexLocked(),
		EndOffset:   s.endOff,
		IORing:      s.opts.IORing,
	}
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
