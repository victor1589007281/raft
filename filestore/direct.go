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
	"fmt"
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
//
// 12.14 锁税根治(#19 lurch 引擎): 旧实现全程持 mu(含 WriteAt+fdatasync)——
// 高直发速率下 follower 的 mu 被直发打满, raft AppendEntries 的 StoreLogs
// 排不上 → 提交往返膨胀 → 计算层流水线截止重试 → 自激 lurch。现两阶段:
// 锁内只做到位判定+偏移占位(纯内存), WriteAt+fdatasync 移出大锁
// (append-only 不可变偏移; fdatasync 并发幂等安全), 发布后段再回锁内。
func (s *Store) WriteRedoDirect(base, lsn uint64, payload []byte) error {
	// 压实换刀重试: 占位→锁外写→发布三阶段之间若 CompactRedo 换了 fd,
	// 我的未发布记录不会被带进新文件(压实只搬 redoMap 已发布者) ——
	// 发布前校验 fd 身份, 变了就以新 fd 重走全流程(去重判定在新 map 上重做)。
	for attempt := 0; ; attempt++ {
		if attempt >= 3 {
			return fmt.Errorf("filestore: direct lsn=%d 连续撞上压实换刀, 放弃(由 raft 路径承载)", lsn)
		}
		// ---- 阶段 1(锁内): 模式/去重判定 + 偏移占位 + 未绑定水位记账 ----
		s.mu.Lock()
		if s.format != "v2-ref" || s.redoF == nil {
			s.mu.Unlock()
			return ErrNotRefMode
		}
		if _, ok := s.redoMap[lsn]; ok {
			// 与 storeLogsRef 同构的流快照语义: 同 LSN 重写 = 同一块的更新快照
			// (块渐满扩展 / 块头 data_len 推进), 跨 epoch 脑裂已被 RPC fencing 拦截。
			// 直发快照通常早于绑定快照, 故多数命中走"旧快照迟到 → 保留已存"。
			existing, rerr := s.readRedoPayload(lsn)
			if rerr == nil {
				diff := firstDiffAt(existing, payload)
				switch {
				case diff == -1:
					s.mu.Unlock()
					return nil // 完全相等: 幂等
				case len(payload) <= len(existing) && (diff == len(payload) || diff < redoBlockHeaderLen):
					s.mu.Unlock()
					return nil // 旧快照迟到(更短/同长): 保留已存的更新版本
				case len(payload) > len(existing) && (diff == len(existing) || diff < redoBlockHeaderLen):
					// 更新快照: 落到下面重写
				default:
					if _, bound := s.lsnIndex[lsn]; bound {
						s.mu.Unlock()
						return fmt.Errorf("filestore: direct lsn=%d 与已绑定内容真分叉(firstDiff=%d existLen=%d newLen=%d) —— fencing", lsn, diff, len(existing), len(payload))
					}
					if s.opts.Logger != nil {
						s.opts.Logger.Printf("[WARN] filestore: direct lsn=%d 未绑定异字节重写(故障切换修复) (existLen=%d newLen=%d firstDiff=%d)", lsn, len(existing), len(payload), diff)
					}
				}
			}
		}
		framer := s.opts.Framer
		if framer == nil {
			framer = StdFramer{}
		}
		body := framer.Frame(base, lsn, payload)
		recLen := 4 + len(body)
		if s.opts.UnboundRedoLimit > 0 && s.unboundBytes+int64(recLen) > int64(s.opts.UnboundRedoLimit) {
			s.mu.Unlock()
			return ErrUnboundFull
		}
		// One-record batch, 512B tail-padded (keeps the batch-boundary invariant).
		buf := make([]byte, 4+len(body))
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(body)))
		copy(buf[4:], body)
		// 偏移占位: 立即推进 redoEnd(并发直发各自拿到互异区间), 记账同步预留。
		off := s.redoEnd
		if pad := align512(int64(len(buf))) - int64(len(buf)); pad > 0 {
			buf = append(buf, make([]byte, pad)...)
		}
		s.redoEnd = off + align512(int64(len(buf)))
		s.unboundBytes += int64(recLen)
		f := s.redoF
		// 持链移交(与 pinRefReadLocked 同构): 仍持 mu 时取 redoIoMu.RLock ——
		// 压实换刀需 mu.Lock+redoIoMu.Lock, 此移交令换刀绝不可能在我的
		// WriteAt 前完成 close(否则会 EBADF —— -race 烤机实测抓获)。
		s.redoIoMu.RLock()
		s.mu.Unlock()

		rollbackReservation := func() {
			s.mu.Lock()
			s.unboundBytes -= int64(recLen)
			if s.unboundBytes < 0 {
				s.unboundBytes = 0
			}
			s.mu.Unlock()
		}

		// ---- 阶段 2(redoIoMu.RLock 内, 阶段 1 已持链移交): 写盘 + 屏障 ——
		// 钉住 fd 防压实换刀中途 close; append-only 不可变偏移, fdatasync 并发幂等安全。
		wbuf := buf
		if s.directIO {
			wbuf = alignedCopy(buf)
		}
		durable := false
		usedIORing := false
		var werr error
		if s.opts.IORing {
			if err := storeLogsIORingChain(f, off, [][]byte{wbuf}, s.opts.IORingSQPoll); err == nil {
				durable = true
				usedIORing = true
			}
		}
		if !durable {
			if _, err := f.WriteAt(wbuf, off); err != nil {
				werr = err
			} else if err := s.barrierOn(f); err != nil {
				werr = err
			}
		}
		s.redoIoMu.RUnlock()
		if werr != nil {
			rollbackReservation() // 占位区间留零填充洞, 扫描器按批尾填充跳过
			return werr
		}

		// ---- 阶段 3(锁内): 发布映射; fd 已被压实换掉则整体重来 ----
		s.mu.Lock()
		if s.redoF != f {
			s.unboundBytes -= int64(recLen)
			if s.unboundBytes < 0 {
				s.unboundBytes = 0
			}
			s.mu.Unlock()
			continue // 压实把未发布记录留在旧 inode —— 必须以新 fd 重写
		}
		if usedIORing {
			s.ioringOK++
		}
		if s.redoMap == nil {
			s.redoMap = make(map[uint64]redoPos)
		}
		s.redoMap[lsn] = redoPos{off: off, length: recLen}
		s.mu.Unlock()
		return nil
	}
}

// elideMinBytes: don't bother eliding tiny entries (marker ≈ 24B+8B/lsn).
const elideMinBytes = 128

// ElideLogData implements raft.LogDataElider (leader side, P4): a redo entry
// big enough to matter goes out as a pointer copy — Data elided, Extensions
// carries [base][lsns][dataLen][crc]. The original entry is untouched.
func (s *Store) ElideLogData(l *raft.Log) (*raft.Log, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
// data for this follower). 两阶段: 抓拍在 mu.RLock, 持链移交 redoIoMu.RLock
// 后锁外读盘。
func (s *Store) ResolveElidedData(ext []byte) ([]byte, error) {
	m := raft.DecodeElideMarker(ext)
	if m == nil {
		return nil, errors.New("filestore: bad elide marker")
	}
	s.mu.RLock()
	if s.format != "v2-ref" {
		s.mu.RUnlock()
		return nil, ErrNotRefMode
	}
	f, poss, err := s.pinRefReadLocked(m.LSNs)
	if err != nil {
		// mu 已放。逐段区分缺失 LSN(回执要带首个缺失段): 缺失 = redoMap 无此
		// LSN(直发尚未到达), 由 leader 回退全量重发。
		s.mu.RLock()
		miss := uint64(0)
		for _, lsn := range m.LSNs {
			if _, ok := s.redoMap[lsn]; !ok {
				miss = lsn
				break
			}
		}
		s.mu.RUnlock()
		if miss != 0 {
			return nil, &raft.RedoDataMissingError{LSN: miss}
		}
		return nil, err
	}
	segs := make([]RedoSegment, len(m.LSNs))
	for i, lsn := range m.LSNs {
		payload, rerr := s.readRedoAt(f, poss[i])
		if rerr != nil {
			s.redoIoMu.RUnlock()
			return nil, &raft.RedoDataMissingError{LSN: lsn}
		}
		segs[i] = RedoSegment{LSN: lsn, Payload: payload}
	}
	s.redoIoMu.RUnlock()
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
