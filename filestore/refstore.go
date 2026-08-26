// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

// 12.14.3 ref mode: raft log data is a POINTER, not a copy.
//
// Two files, one transaction:
//   raft.log (meta): 512B VWAL header + length-prefixed records.
//     Record = [u32 recLen][64B head][optional tail].
//     Head:  [u64 index][u64 term][u16 type][u16 flags]
//            [u64 base][u64 lsn][u64 appendedAtUnixNano][12B reserved][u32 crc32c over 0:56)[u32 zero]
//     flags: bit0 hasTail (variable tail follows), bit1 isRedo (entry's Data
//            lives in redo.log).
//     tail = [u32 tailLen][msgpack(logTail{Data,Extensions,LSNs})][u32 crc32c].
//     Redo entries carry no Data in meta; multi-segment entries (e.g. LogDB
//     cmdBatch) carry the segment LSN list in the tail. Non-redo entries
//     (config/noop/lease/checkpoint) keep Data inline in the tail.
//   redo.log (data): 512B VWAL header + [u32 len][framer body] records,
//     batch-tail 512B padded. Record bytes are business-defined via
//     RedoFramer (default StdFramer = [u64 base][u64 lsn][payload][u32 crc]).
//
// Multi-segment entries: one raft entry may fold N redo segments (LogDB
// WriteRedoBatcher). Each segment becomes one redo.log record; the meta head
// stores the first LSN, the tail stores all LSNs (order preserved), and both
// the sparse index and the presence map cover every segment.
//
// Crash reconciliation (12.14.3 ③): redo.log is the data authority — meta
// records referencing redo bytes beyond redo's good tail are dropped (meta
// truncated at that point); orphan redo records (written before a torn meta)
// are harmless and reclaimed by CompactRedo.
//
// DeleteRange truncates ONLY the meta file: LSN is not monotonic in index
// order (batched commits fold non-contiguous segments), so redo.log space is
// reclaimed by CompactRedo(keepFromLSN) driven by the business checkpoint,
// never by raft conflict truncation.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

const (
	metaHeadLen  = 64
	metaFlagTail = 1 << 0 // has variable tail (inline Data / Extensions / LSN list)
	metaFlagRedo = 1 << 1 // redo entry: Data lives in redo.log (pointer mode)

	fileFlagRef  = 1 << 2 // raft.log is ref-mode meta
	fileFlagData = 1 << 3 // redo.log is ref-mode data

	redoFileName = "redo.log"
)

// logTail is the msgpack-encoded variable tail of a meta record.
type logTail struct {
	Data       []byte   // inline payload (non-redo entries)
	Extensions []byte   // raft Log.Extensions passthrough
	LSNs       []uint64 // multi-segment redo entries: all segment LSNs
}

// ---------------------------------------------------------------------------
// RedoFramer: business-defined redo record bytes (12.14 "redo 自定义写入").
// The record on disk is [u32 bodyLen][Frame output]; Frame must therefore be
// self-delimiting and SHOULD carry a checksum — Unframe errors are treated as
// torn-tail (truncation point) on reload.
type RedoFramer interface {
	Frame(base, lsn uint64, payload []byte) []byte
	Unframe(body []byte) (base, lsn uint64, payload []byte, err error)
}

// StdFramer is the default framing: [u64 base][u64 lsn][payload][u32 crc32c].
type StdFramer struct{}

func (StdFramer) Frame(base, lsn uint64, payload []byte) []byte {
	body := make([]byte, 16+len(payload)+4)
	binary.LittleEndian.PutUint64(body[0:8], base)
	binary.LittleEndian.PutUint64(body[8:16], lsn)
	copy(body[16:], payload)
	binary.LittleEndian.PutUint32(body[16+len(payload):], crc32.Checksum(body[:16+len(payload)], castagnoli))
	return body
}

func (StdFramer) Unframe(body []byte) (uint64, uint64, []byte, error) {
	if len(body) < 20 {
		return 0, 0, nil, errors.New("short redo record")
	}
	want := binary.LittleEndian.Uint32(body[len(body)-4:])
	if crc32.Checksum(body[:len(body)-4], castagnoli) != want {
		return 0, 0, nil, errors.New("redo crc mismatch")
	}
	base := binary.LittleEndian.Uint64(body[0:8])
	lsn := binary.LittleEndian.Uint64(body[8:16])
	return base, lsn, append([]byte(nil), body[16:len(body)-4]...), nil
}

// ---------------------------------------------------------------------------
// Meta record encode/decode.

type metaHead struct {
	index      uint64
	term       uint64
	logType    uint16
	flags      uint16
	base       uint64
	lsn        uint64 // first segment LSN (0 for non-redo entries)
	appendedAt int64  // unixnano; 0 = unset
}

func encodeMetaRecord(h metaHead, tail *logTail) []byte {
	if tail != nil && len(tail.Data) == 0 && len(tail.Extensions) == 0 && len(tail.LSNs) == 0 {
		tail = nil
	}
	var tailBuf []byte
	if tail != nil {
		h.flags |= metaFlagTail
		if err := codec.NewEncoderBytes(&tailBuf, &msgpackHandle).Encode(tail); err != nil {
			return nil
		}
	}
	recLen := metaHeadLen + boolLen(tail != nil, 4+len(tailBuf)+4)
	body := make([]byte, 4+recLen)
	binary.LittleEndian.PutUint32(body[0:4], uint32(recLen))
	binary.LittleEndian.PutUint64(body[4:12], h.index)
	binary.LittleEndian.PutUint64(body[12:20], h.term)
	binary.LittleEndian.PutUint16(body[20:22], h.logType)
	binary.LittleEndian.PutUint16(body[22:24], h.flags)
	binary.LittleEndian.PutUint64(body[24:32], h.base)
	binary.LittleEndian.PutUint64(body[32:40], h.lsn)
	binary.LittleEndian.PutUint64(body[40:48], uint64(h.appendedAt))
	binary.LittleEndian.PutUint32(body[4+56:4+60], crc32.Checksum(body[4:4+56], castagnoli))
	if tail != nil {
		p := body[4+metaHeadLen:]
		binary.LittleEndian.PutUint32(p[0:4], uint32(len(tailBuf)))
		copy(p[4:], tailBuf)
		binary.LittleEndian.PutUint32(p[4+len(tailBuf):], crc32.Checksum(tailBuf, castagnoli))
	}
	return body
}

func boolLen(b bool, n int) int {
	if b {
		return n
	}
	return 0
}

// decodeMetaRecord parses one record body (bytes after the u32 recLen).
func decodeMetaRecord(body []byte) (metaHead, *logTail, error) {
	var h metaHead
	if len(body) < metaHeadLen {
		return h, nil, errors.New("short meta head")
	}
	want := binary.LittleEndian.Uint32(body[56:60])
	if crc32.Checksum(body[:56], castagnoli) != want {
		return h, nil, errors.New("meta crc mismatch")
	}
	h.index = binary.LittleEndian.Uint64(body[0:8])
	h.term = binary.LittleEndian.Uint64(body[8:16])
	h.logType = binary.LittleEndian.Uint16(body[16:18])
	h.flags = binary.LittleEndian.Uint16(body[18:20])
	h.base = binary.LittleEndian.Uint64(body[20:28])
	h.lsn = binary.LittleEndian.Uint64(body[28:36])
	h.appendedAt = int64(binary.LittleEndian.Uint64(body[36:44]))
	if h.flags&metaFlagTail == 0 {
		return h, nil, nil
	}
	if len(body) < metaHeadLen+4 {
		return h, nil, errors.New("short tail len")
	}
	tailLen := int(binary.LittleEndian.Uint32(body[metaHeadLen:]))
	if len(body) != metaHeadLen+4+tailLen+4 {
		return h, nil, errors.New("bad tail framing")
	}
	tailBytes := body[metaHeadLen+4 : metaHeadLen+4+tailLen]
	wantCrc := binary.LittleEndian.Uint32(body[metaHeadLen+4+tailLen:])
	if crc32.Checksum(tailBytes, castagnoli) != wantCrc {
		return h, nil, errors.New("tail crc mismatch")
	}
	t := &logTail{}
	if err := codec.NewDecoderBytes(tailBytes, &msgpackHandle).Decode(t); err != nil {
		return h, nil, err
	}
	return h, t, nil
}

// ---------------------------------------------------------------------------
// Ref-mode store paths.

// refEntrySegments returns the segment LSN list of entry i (nil if inline).
func (s *Store) refEntrySegments(i int) []uint64 {
	if i >= len(s.refSegs) {
		return nil
	}
	return s.refSegs[i]
}

// segRef tracks where each segment of an entry lives in redo.log: freshly
// written by this batch (wrote) or already durable from a P3 direct write /
// idempotent resend (skip the rewrite — the single-copy point under P3).
type segRef struct {
	lsn    uint64
	off    int64
	length int
	wrote  bool
}

// storeLogsRef persists a batch as: redo segments appended to redo.log,
// fixed meta records appended to raft.log, both durable in one io_uring
// transaction (WRITE(redo)→WRITE(meta)→FSYNC→FSYNC) or the classic fallback
// (write×2 + barrier×2). Segments already present in redoMap are not
// rewritten (idempotent dedup; their durability came from the direct path's
// own barrier).
func (s *Store) storeLogsRef(logs []*raft.Log) error {
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	var redoBuf, metaBuf bytes.Buffer
	metas := make([]*raft.Log, len(logs)) // in-memory view: Data stripped for ref entries
	segLists := make([][]uint64, len(logs))
	segRefs := make([][]segRef, len(logs))
	bases := make([]uint64, len(logs))

	for i, l := range logs {
		base, segs := s.splitEntry(l)
		bases[i] = base
		at := int64(0)
		if !l.AppendedAt.IsZero() {
			at = l.AppendedAt.UnixNano()
		}
		h := metaHead{index: l.Index, term: l.Term, logType: uint16(l.Type), appendedAt: at}
		m := *l
		if len(segs) > 0 {
			// Redo entry: each segment becomes one redo.log record; meta keeps
			// only the pointer (first LSN in head, full list in tail if >1).
			h.flags |= metaFlagRedo
			h.base, h.lsn = base, segs[0].LSN
			segLSNs := make([]uint64, len(segs))
			srefs := make([]segRef, len(segs))
			for j, seg := range segs {
				segLSNs[j] = seg.LSN
				if pos, ok := s.redoMap[seg.LSN]; ok {
					// P3 dedup: bytes already durable from the direct path.
					// redo 语义同 LSN 同字节 —— 但故障切换窗口里旧写者可能留下同 LSN
					// 异字节(脑裂残余): 内容不等时, 未被绑定的重写(新写者修复),
					// 已被绑定的不动(committed 内容不可改写, 记 invariant 违例)。
					existing, rerr := s.readRedoPayload(seg.LSN)
					switch {
					case rerr == nil && bytes.Equal(existing, seg.Payload):
						srefs[j] = segRef{lsn: seg.LSN, off: pos.off, length: pos.length}
						if s.unboundBytes > 0 {
							s.unboundBytes -= int64(pos.length)
							if s.unboundBytes < 0 {
								s.unboundBytes = 0
							}
						}
						continue
					case rerr == nil:
						if _, bound := s.lsnIndex[seg.LSN]; bound {
							if s.opts.Logger != nil {
								s.opts.Logger.Printf("[ERROR] filestore: lsn=%d 已绑定但内容异变 —— fencing 违例, 保留已提交字节 (existLen=%d newLen=%d firstDiff=%d newBase=%d)", seg.LSN, len(existing), len(seg.Payload), firstDiffAt(existing, seg.Payload), base)
							}
							srefs[j] = segRef{lsn: seg.LSN, off: pos.off, length: pos.length}
							continue
						}
						if s.opts.Logger != nil {
							s.opts.Logger.Printf("[WARN] filestore: lsn=%d 未绑定异字节重写(故障切换修复) (existLen=%d newLen=%d firstDiff=%d)", seg.LSN, len(existing), len(seg.Payload), firstDiffAt(existing, seg.Payload))
						}
						// fallthrough to rewrite below
					}
					// rerr != nil (存在映射但读不出): 视为缺失, 重写
				}
				body := framer.Frame(base, seg.LSN, seg.Payload)
				var rec [4]byte
				binary.LittleEndian.PutUint32(rec[:], uint32(len(body)))
				srefs[j] = segRef{lsn: seg.LSN, off: s.redoEnd + int64(redoBuf.Len()), length: 4 + len(body), wrote: true}
				redoBuf.Write(rec[:])
				redoBuf.Write(body)
			}
			m.Data = nil
			var tail *logTail
			if len(segLSNs) > 1 || len(l.Extensions) > 0 {
				tail = &logTail{Extensions: l.Extensions, LSNs: segLSNs}
			}
			metaBuf.Write(encodeMetaRecord(h, tail))
			segLists[i] = segLSNs
			segRefs[i] = srefs
		} else {
			// Non-redo entry (config/noop/lease/checkpoint): Data inline.
			metaBuf.Write(encodeMetaRecord(h, &logTail{Data: l.Data, Extensions: l.Extensions}))
		}
		metas[i] = &m
	}
	// Batch-tail padding: both files keep 512-aligned batch boundaries.
	if pad := align512(int64(redoBuf.Len())) - int64(redoBuf.Len()); redoBuf.Len() > 0 && pad > 0 {
		redoBuf.Write(make([]byte, pad))
	}
	if pad := align512(int64(metaBuf.Len())) - int64(metaBuf.Len()); pad > 0 {
		metaBuf.Write(make([]byte, pad))
	}
	redoBytes, metaBytes := redoBuf.Bytes(), metaBuf.Bytes()
	if s.directIO {
		redoBytes = alignedCopy(redoBytes)
		metaBytes = alignedCopy(metaBytes)
	}

	baseOff, redoBaseOff := s.endOff, s.redoEnd
	durable := false
	if s.opts.IORing {
		if err := storeLogsIORingDual(s.redoF, redoBaseOff, redoBytes, s.f, baseOff, metaBytes, s.opts.IORingSQPoll); err == nil {
			durable = true
			s.ioringOK++
		}
	}
	if !durable {
		if len(redoBytes) > 0 {
			if _, err := s.redoF.WriteAt(redoBytes, redoBaseOff); err != nil {
				return err
			}
			if err := s.barrierOn(s.redoF); err != nil {
				return err
			}
		}
		if _, err := s.f.WriteAt(metaBytes, baseOff); err != nil {
			return err
		}
		if err := s.barrierOn(s.f); err != nil {
			return err
		}
	}

	// Commit the in-memory view. Walk metaBytes for per-record meta offsets;
	// per-segment presence entries come from the segRefs built above.
	metaOff := baseOff
	for i := range logs {
		if s.first == 0 {
			s.first = metas[i].Index
		}
		s.logs = append(s.logs, metas[i])
		s.offsets = append(s.offsets, metaOff)
		s.refSegs = append(s.refSegs, segLists[i])
		s.refBases = append(s.refBases, bases[i])
		recLen := int64(binary.LittleEndian.Uint32(metaBytes[metaOff-baseOff:])) + 4
		metaOff += recLen
		for _, sr := range segRefs[i] {
			if s.redoMap == nil {
				s.redoMap = make(map[uint64]redoPos)
			}
			if sr.wrote {
				s.redoMap[sr.lsn] = redoPos{off: sr.off, length: sr.length}
			}
			// ref mode: the LSN index IS the read path (LookupByLSN/ReadRedo),
			// so it is built unconditionally, not gated on SparseIndex.
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[sr.lsn] = lsnPos{index: metas[i].Index, off: sr.off}
		}
	}
	s.endOff = baseOff + align512(int64(len(metaBytes)))
	if len(redoBytes) > 0 {
		s.redoEnd = redoBaseOff + align512(int64(len(redoBytes)))
	}
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
		if s.redoF != nil {
			_ = preallocate(s.redoF, s.redoEnd, s.opts.SegmentSize)
		}
	}
	if s.opts.UseSyncFileRange {
		_ = syncFileRange(s.f, 0, 0)
	}
	return nil
}

// splitEntry decides an entry's redo segments: the multi-segment codec wins;
// the plain codec yields ≤1 segment (lsn!=0); no codec = no segments.
func (s *Store) splitEntry(l *raft.Log) (uint64, []RedoSegment) {
	if sc, ok := s.opts.Codec.(RedoSegmentCodec); ok {
		return sc.SplitSegments(l.Data)
	}
	if s.opts.Codec != nil {
		base, lsn, payload := s.opts.Codec.Split(l)
		if lsn != 0 {
			return base, []RedoSegment{{LSN: lsn, Payload: payload}}
		}
	}
	return 0, nil
}

// mergeEntry rebuilds Data for a redo entry from its segments.
func (s *Store) mergeEntry(base uint64, segLSNs []uint64) ([]byte, error) {
	segs := make([]RedoSegment, len(segLSNs))
	for j, lsn := range segLSNs {
		payload, err := s.readRedoPayload(lsn)
		if err != nil {
			return nil, err
		}
		segs[j] = RedoSegment{LSN: lsn, Payload: payload}
	}
	if sc, ok := s.opts.Codec.(RedoSegmentCodec); ok {
		return sc.MergeSegments(base, segs), nil
	}
	if s.opts.Codec != nil && len(segs) == 1 {
		return s.opts.Codec.Merge(base, segs[0].LSN, segs[0].Payload), nil
	}
	if s.opts.Codec == nil && len(segs) == 1 {
		return segs[0].Payload, nil
	}
	return nil, fmt.Errorf("filestore: cannot merge %d segments without RedoSegmentCodec", len(segs))
}

// reloadRef scans redo.log (data authority) then raft.log (meta), dropping
// meta entries whose redo bytes were torn away.
func (s *Store) reloadRef(rawMeta, rawRedo []byte) error {
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	// 1. redo scan: build the lsn→(off,len) presence map, find the good tail.
	redoGood := int64(fileHeaderLen)
	if len(rawRedo) >= fileHeaderLen {
		redoGood, _ = scanRecords(rawRedo, fileHeaderLen, true, func(off int64, body []byte) error {
			_, lsn, _, err := framer.Unframe(body)
			if err != nil {
				return errScanStop // torn/corrupt redo tail
			}
			if s.redoMap == nil {
				s.redoMap = make(map[uint64]redoPos)
			}
			s.redoMap[lsn] = redoPos{off: off, length: 4 + len(body)}
			return nil
		})
	}
	// 2. meta scan: rebuild logs; drop entries referencing torn redo bytes.
	metaGood, err := scanRecords(rawMeta, fileHeaderLen, true, func(off int64, body []byte) error {
		h, tail, err := decodeMetaRecord(body)
		if err != nil {
			return errScanStop
		}
		l := &raft.Log{Index: h.index, Term: h.term, Type: raft.LogType(h.logType)}
		if h.appendedAt != 0 {
			l.AppendedAt = timeFromUnixNano(h.appendedAt)
		}
		var segLSNs []uint64
		if h.flags&metaFlagRedo != 0 {
			segLSNs = []uint64{h.lsn}
			if tail != nil && len(tail.LSNs) > 0 {
				segLSNs = tail.LSNs
			}
			// redo 为准: every referenced segment must be within the good tail.
			for _, lsn := range segLSNs {
				pos, ok := s.redoMap[lsn]
				if !ok || pos.off+int64(pos.length) > redoGood {
					return errScanStop
				}
			}
			for _, lsn := range segLSNs {
				if s.lsnIndex == nil {
					s.lsnIndex = make(map[uint64]lsnPos)
				}
				s.lsnIndex[lsn] = lsnPos{index: h.index, off: s.redoMap[lsn].off}
			}
		}
		if tail != nil && len(tail.Data) > 0 {
			l.Data = tail.Data
		}
		if tail != nil && len(tail.Extensions) > 0 {
			l.Extensions = tail.Extensions
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, off)
		s.refSegs = append(s.refSegs, segLSNs)
		s.refBases = append(s.refBases, h.base)
		return nil
	})
	if err != nil {
		return err
	}
	s.endOff = metaGood
	s.redoEnd = redoGood
	s.truncateAtGapLocked()
	return nil
}

// readRedoPayload returns the payload bytes for an lsn via the presence map.
func (s *Store) readRedoPayload(lsn uint64) ([]byte, error) {
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	pos, ok := s.redoMap[lsn]
	if !ok {
		return nil, raft.ErrLogNotFound
	}
	var raw []byte
	if s.directIO {
		r, err := readAtAligned(s.redoF, pos.off, pos.length)
		if err != nil {
			return nil, err
		}
		raw = r
	} else {
		raw = make([]byte, pos.length)
		if _, err := s.redoF.ReadAt(raw, pos.off); err != nil {
			return nil, err
		}
	}
	if len(raw) < 4 {
		return nil, errors.New("short redo read")
	}
	bodyLen := int(binary.LittleEndian.Uint32(raw[:4]))
	if bodyLen+4 > len(raw) {
		return nil, errors.New("bad redo framing")
	}
	_, _, payload, err := framer.Unframe(raw[4 : 4+bodyLen])
	return payload, err
}

// firstDiffAt 返回两个字节串首个不同字节的偏移; 前缀相等但长度不同返回较短长度;
// 完全相等返回 -1。仅供 fencing 诊断日志使用。
func firstDiffAt(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

// ---------------------------------------------------------------------------
// Compaction (checkpoint-driven prefix GC; NOT DeleteRange — see file header).

// CompactRedo drops redo records with lsn < keepFromLSN by rewriting the
// survivors to a fresh file and renaming it over redo.log. keepFrom is
// clamped to the smallest lsn still referenced by live meta entries, so
// committed-and-referenced redo is never dropped. Callers must drive this
// from the business checkpoint (raft log snapshot must cover the dropped
// range first).
func (s *Store) CompactRedo(keepFromLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.format != "v2-ref" || s.redoF == nil {
		return nil
	}
	for _, segs := range s.refSegs {
		for _, lsn := range segs {
			if lsn < keepFromLSN {
				keepFromLSN = lsn
			}
		}
	}
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	dir := filepath.Dir(s.path)
	tmpPath := filepath.Join(dir, redoFileName+".tmp")
	nf, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeFileHeader(nf, fileFlagSingleWAL|fileFlagPad512|fileFlagRef|fileFlagData); err != nil {
		nf.Close()
		return err
	}
	newMap := make(map[uint64]redoPos)
	writeOff := int64(fileHeaderLen)
	var batch bytes.Buffer
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		if pad := align512(int64(batch.Len())) - int64(batch.Len()); pad > 0 {
			batch.Write(make([]byte, pad))
		}
		if _, err := nf.WriteAt(batch.Bytes(), writeOff); err != nil {
			return err
		}
		writeOff += int64(batch.Len())
		batch.Reset()
		return nil
	}
	lsns := make([]uint64, 0, len(s.redoMap))
	for lsn := range s.redoMap {
		lsns = append(lsns, lsn)
	}
	sortU64(lsns)
	for _, lsn := range lsns {
		if lsn < keepFromLSN {
			continue
		}
		pos := s.redoMap[lsn]
		raw := make([]byte, pos.length)
		if _, err := s.redoF.ReadAt(raw, pos.off); err != nil {
			nf.Close()
			return err
		}
		bodyLen := int(binary.LittleEndian.Uint32(raw[:4]))
		if _, _, _, err := framer.Unframe(raw[4 : 4+bodyLen]); err != nil {
			nf.Close()
			return fmt.Errorf("compact: redo@%d corrupt: %w", pos.off, err)
		}
		newMap[lsn] = redoPos{off: writeOff + int64(batch.Len()), length: pos.length}
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
	if err := os.Rename(tmpPath, filepath.Join(dir, redoFileName)); err != nil {
		return err
	}
	syncDir(dir)
	newF, err := os.OpenFile(filepath.Join(dir, redoFileName), os.O_RDWR|openDirectFlag(s.directIO), 0o644)
	if err != nil {
		return err
	}
	_ = s.redoF.Close()
	s.redoF = newF
	s.redoMap = newMap
	s.redoEnd = writeOff
	return nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

func sortU64(a []uint64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// ---------------------------------------------------------------------------
// Alignment helpers (O_DIRECT tier).

// alignedCopy returns a 512-aligned copy of b (for O_DIRECT writes; lengths
// and offsets are already aligned by batch-tail padding).
func alignedCopy(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	buf := alignedBytes(len(b))
	copy(buf, b)
	return buf
}

func alignedBytes(n int) []byte {
	raw := make([]byte, n+511)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	pad := int((512 - (base & 511)) & 511)
	return raw[pad : pad+n]
}

// readAtAligned services an arbitrary (off,len) read from an O_DIRECT fd.
func readAtAligned(f *os.File, off int64, n int) ([]byte, error) {
	lo := off &^ 511
	hi := align512(off + int64(n))
	buf := alignedBytes(int(hi - lo))
	got, err := f.ReadAt(buf, lo)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int64(got) < off-lo+int64(n) {
		return nil, io.ErrUnexpectedEOF
	}
	return buf[off-lo : off-lo+int64(n)], nil
}

func openDirectFlag(on bool) int {
	if on {
		return unix.O_DIRECT
	}
	return 0
}

func timeFromUnixNano(ns int64) time.Time { return time.Unix(0, ns) }
