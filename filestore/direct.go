// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

// 12.14 P3/P4 store 侧能力(ref 模式):
//
//   P3 直发数据面: WriteRedoDirect —— 计算层多播, 每副本把 redo 段直接写进
//     redo.log(不经 raft), 自带 fdatasync 屏障; 幂等(同 LSN 同字节);
//     未绑定字节记账 + UnboundRedoLimit 背压(超限 → ErrUnboundFull, 调用方
//     降级回正常路径)。后续 raft StoreLogs 对已存在段跳过重写(单副本语义
//     真正成立: 直发承载数据写, raft 只补定长元数据)。
//   P4 指针级复制: ElideLogData(leader 省略 Data, Extensions 携带指针) /
//     ResolveElidedData(follower 从本地 redo.log 按 LSN 读回)。raft 核心在
//     存储前完成解析, 无法解析的存储(legacy/inmem/boltdb)经
//     RedoDataMissingError 让 leader 回退全量 —— 混合集群安全。

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/hashicorp/raft"
)

// ErrNotRefMode is returned by P3/P4 APIs on non-ref stores.
var ErrNotRefMode = errors.New("filestore: ref mode required")

// ErrUnboundFull signals the unbound-direct-byte limit is reached (P3
// backpressure); the caller should stop the direct fan-out and use the
// normal leader path until raft binding drains the backlog.
var ErrUnboundFull = errors.New("filestore: unbound redo buffer full")

// WriteRedoDirect appends one redo segment to redo.log without a raft entry
// (P3 data plane). Durable on return (own barrier). Idempotent: the same LSN
// re-delivered is a no-op. The record is UNBOUND until a committed raft meta
// entry references it — readers must gate on the committed frontier (LogDB's
// FSM image does; ReadRedo is a presence-level API).
func (s *Store) WriteRedoDirect(base, lsn uint64, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.format != "v2-ref" || s.redoF == nil {
		return ErrNotRefMode
	}
	if _, ok := s.redoMap[lsn]; ok {
		return nil // idempotent: same LSN ⇒ same bytes (redo semantics)
	}
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	body := framer.Frame(base, lsn, payload)
	recLen := 4 + len(body)
	if s.opts.UnboundRedoLimit > 0 && s.unboundBytes+int64(recLen) > int64(s.opts.UnboundRedoLimit) {
		return ErrUnboundFull
	}
	// One-record batch, 512B tail-padded (keeps the batch-boundary invariant).
	buf := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(body)))
	copy(buf[4:], body)
	off := s.redoEnd
	if pad := align512(int64(len(buf))) - int64(len(buf)); pad > 0 {
		buf = append(buf, make([]byte, pad)...)
	}
	wbuf := buf
	if s.directIO {
		wbuf = alignedCopy(buf)
	}
	durable := false
	if s.opts.IORing {
		if err := storeLogsIORingChain(s.redoF, off, [][]byte{wbuf}, s.opts.IORingSQPoll); err == nil {
			durable = true
			s.ioringOK++
		}
	}
	if !durable {
		if _, err := s.redoF.WriteAt(wbuf, off); err != nil {
			return err
		}
		if err := s.barrierOn(s.redoF); err != nil {
			return err
		}
	}
	if s.redoMap == nil {
		s.redoMap = make(map[uint64]redoPos)
	}
	s.redoMap[lsn] = redoPos{off: off, length: recLen}
	s.redoEnd = off + align512(int64(len(buf)))
	s.unboundBytes += int64(recLen)
	return nil
}

// elideMinBytes: don't bother eliding tiny entries (marker ≈ 24B+8B/lsn).
const elideMinBytes = 128

// ElideLogData implements raft.LogDataElider (leader side, P4): a redo entry
// big enough to matter goes out as a pointer copy — Data elided, Extensions
// carries [base][lsns][dataLen][crc]. The original entry is untouched.
func (s *Store) ElideLogData(l *raft.Log) (*raft.Log, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.format != "v2-ref" || s.opts.Codec == nil {
		return nil, false
	}
	if len(l.Data) < elideMinBytes || len(l.Extensions) > 0 {
		return nil, false
	}
	base, segs := s.splitEntry(l)
	if len(segs) == 0 {
		return nil, false
	}
	lsns := make([]uint64, len(segs))
	for i, seg := range segs {
		lsns[i] = seg.LSN
	}
	cp := *l
	cp.Data = nil
	cp.Extensions = raft.EncodeElideMarker(base, lsns, uint32(len(l.Data)), crc32.Checksum(l.Data, castagnoli))
	return &cp, true
}

// ResolveElidedData implements raft.RedoByteResolver (follower side, P4):
// rebuild the elided entry's Data from the local redo file by LSN. Any
// missing segment → *raft.RedoDataMissingError (leader falls back to full
// data for this follower).
func (s *Store) ResolveElidedData(ext []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := raft.DecodeElideMarker(ext)
	if m == nil {
		return nil, errors.New("filestore: bad elide marker")
	}
	if s.format != "v2-ref" {
		return nil, ErrNotRefMode
	}
	segs := make([]RedoSegment, len(m.LSNs))
	for i, lsn := range m.LSNs {
		payload, err := s.readRedoPayload(lsn)
		if err != nil {
			return nil, &raft.RedoDataMissingError{LSN: lsn}
		}
		segs[i] = RedoSegment{LSN: lsn, Payload: payload}
	}
	var data []byte
	if sc, ok := s.opts.Codec.(RedoSegmentCodec); ok {
		data = sc.MergeSegments(m.Base, segs)
	} else if s.opts.Codec != nil && len(segs) == 1 {
		data = s.opts.Codec.Merge(m.Base, segs[0].LSN, segs[0].Payload)
	} else {
		return nil, errors.New("filestore: multi-segment resolve needs RedoSegmentCodec")
	}
	if uint32(len(data)) != m.DataLen || crc32.Checksum(data, castagnoli) != m.DataCRC {
		return nil, errors.New("filestore: resolved data crc/len mismatch")
	}
	return data, nil
}
