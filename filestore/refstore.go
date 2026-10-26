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
	"sync/atomic"
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

// Frame 把一段 redo payload 按默认帧格式 [u64 base][u64 lsn][payload][u32 crc32c]
// 编码成记录 body —— 磁盘上是 [u32 bodyLen][body], body 必须自定界。
func (StdFramer) Frame(base, lsn uint64, payload []byte) []byte {
	body := make([]byte, 16+len(payload)+4)
	binary.LittleEndian.PutUint64(body[0:8], base)
	binary.LittleEndian.PutUint64(body[8:16], lsn)
	copy(body[16:], payload)
	binary.LittleEndian.PutUint32(body[16+len(payload):], crc32.Checksum(body[:16+len(payload)], castagnoli))
	return body
}

// Unframe 校验 CRC 后拆出 (base, lsn, payload); 长度不足或 CRC 不符一律报错。
// 该错误在重载侧等价于「撕尾」: reloadRef 的 redo 扫描在此截断 good tail。
func (StdFramer) Unframe(body []byte) (uint64, uint64, []byte, error) {
	// 20 = 8(base) + 8(lsn) + 4(crc): 比这还短连帧头都放不下。
	if len(body) < 20 {
		return 0, 0, nil, errors.New("short redo record")
	}
	want := binary.LittleEndian.Uint32(body[len(body)-4:])
	// CRC 覆盖除末尾 4B 校验和之外的整段; 不过即判撕尾(重载时在此截断)。
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

// encodeMetaRecord 把一条 meta 头(可选 tail)编码为 [u32 recLen][64B head][tail]。
// head 的 crc32c 只覆盖前 56B(56-60 是校验位、60-64 保留零); tail 自带
// [u32 tailLen][msgpack][u32 crc]。msgpack 编码失败返回 nil(调用方按空记录处理)。
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

// boolLen 在 b 为真时返回 n, 否则返回 0 —— 把「可选 tail 是否存在」折算成长度增量。
func boolLen(b bool, n int) int {
	if b {
		return n
	}
	return 0
}

// decodeMetaRecord 解析一条 meta 记录 body(u32 recLen 之后的部分), 依次校验
// head 与 tail 两级 CRC —— 任一级不过都返回错误, 由重载侧当作截断点。
//
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

// refEntrySegments 返回条目 i 的段 LSN 列表(内联条目或越界返回 nil)。
//
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

// storeLogsRef 把一个批次落盘: redo 段追加进 redo.log, 定长 meta 记录追加进
// raft.log, 两者在同一次 io_uring 事务(WRITE(redo)→WRITE(meta)→FSYNC→FSYNC)
// 或经典回退路径(两次写 + 两次屏障)内一起变持久。调用方持 s.mu 写锁。
//
// storeLogsRef persists a batch as: redo segments appended to redo.log,
// fixed meta records appended to raft.log, both durable in one io_uring
// transaction (WRITE(redo)→WRITE(meta)→FSYNC→FSYNC) or the classic fallback
// (write×2 + barrier×2). Segments already present in redoMap are not
// rewritten (idempotent dedup; their durability came from the direct path's
// own barrier).
//
// 算法解读：
//
//	原理：对每条日志先 splitEntry 切成 redo 段; 每段一条 redo.log 记录, meta 头
//	只留指针(首段 LSN 在 head, 多段全表在 tail)。已在 redoMap 里的 LSN 走
//	firstDiffAt 判定, 只有「确认要重写」才追加新记录; 两文件各自按 512 补齐批尾,
//	再交给 io_uring 双 fd 链或经典路径落盘, 最后在内存里发布 logs/offsets/refSegs/
//	redoMap/lsnIndex 与未绑定台账。
//	关键不变量：① 单副本语义 —— 同一 LSN 的字节只应存在一份「当前代」, 判定表
//	(相等/更长且前缀相容/更短且为前缀/真分叉)决定去重、更新、保留还是修复;
//	② redo 先于 meta 持久(链里 FSYNC(redo) 排在 WRITE(meta) 前), 否则崩溃后会
//	出现「meta 指向尚未落盘的 redo 字节」; ③ 内存索引只在两文件都 durable 之后
//	才发布, 失败时 return 早于任何 append; ④ lsnIndex 在 ref 模式无条件建立
//	(它本身就是读路径, 不受 SparseIndex 开关约束)。
//	代价与边界：去重判定要对每个已存在段做一次 readRedoPayload(可能触发 O_DIRECT
//	对齐读), 命中率高时省下写放大、命中率低时白读一次; 批尾 512 填充使每条批次
//	至少占 512B, 小批次空间利用率低。
//	设计权衡：用「读旧 + 比较 + 选择性重写」换「盲写造成同 LSN 双份字节」——
//	redo.log 按位置寻址, 同 LSN 出现两份会让水位与压实语义失效; 但完全不比较
//	又会丢掉「块渐满时同 start 段被更新快照覆盖」的正常演化。故取比较后按类别
//	分派, 并把「未绑定重写」的账统一收口到 commit 循环的 unboundDrain(避免逐分支
//	扣预留造成的泄漏)。
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
					// redo 流按位置寻址, 同 LSN 的重写是"同一快照的更新版本"而非分叉:
					// InnoDB log-writer 对未满 512B 块随数据渐满重写同 start 段, 且块头
					// 12B 内的 data_len 字段(偏移 4-5)随填充推进 —— 直发路径快照早于
					// raft 绑定路径时, 同 LSN 两段在偏移 4/5 分叉是常态而非异常。
					// 跨 epoch 的脑裂双写已被 RPC 层 writer_epoch fencing 拦截, 能到这里
					// 的同 LSN 重写皆同 epoch 的块演化。判定:
					//   完全相等                    → 幂等去重;
					//   更长且(前缀相等 | 头区分叉)  → 更新为更新快照(重写);
					//   更短且(是其前缀 | 头区分叉)  → 旧快照迟到, 保留已存;
					//   头区外真分叉/异常缩短        → 未绑定重写(修复), 已绑定保留
					//     + fencing 违例日志(真脑裂残余仅存此列)。
					// (前史: ①把扩展误判 fencing 保留短记录 → 水位留洞 tps 崩塌;
					//  ②把头区推进也扣在 fencing → 新鲜集群稳态下违例刷屏 + 重建读回旧头。)
					existing, rerr := s.readRedoPayload(seg.LSN)
					if rerr == nil {
						diff := firstDiffAt(existing, seg.Payload)
						_, bound := s.lsnIndex[seg.LSN]
						// J5 泄漏修复(9.24): 各分支不再逐个扣预留 —— 记账统一
						// 收口到下方 commit 循环的 unboundDrain(绑定=终点事件,
						// 整条清账, 同 LSN 多代台账一次清零)。旧实现只有
						// exact-match 按最新代长度扣减, keep/rewrite/fencing
						// 各分支全漏, 同 LSN 块渐满多代重写下 60% 预留永久
						// 泄漏, 水位涨满 UnboundRedoLimit → 直发通道永久背压。
						switch {
						case diff == -1:
							srefs[j] = segRef{lsn: seg.LSN, off: pos.off, length: pos.length}
							continue
						case len(seg.Payload) > len(existing) && (diff == len(existing) || diff < redoBlockHeaderLen):
							if s.opts.Logger != nil {
								s.opts.Logger.Printf("[INFO] filestore: lsn=%d 段快照更新 %d→%d 字节(firstDiff=%d, bound=%v)", seg.LSN, len(existing), len(seg.Payload), diff, bound)
							}
							// 落到重写路径(新记录绑定时刻按其长度统一补齐
							// 绑定量计数; 旧代台账由 commit 循环清账)。
						case len(seg.Payload) <= len(existing) && (diff == len(seg.Payload) || diff < redoBlockHeaderLen):
							// 更短/同长的旧快照迟到: 保留已存(更新)者
							srefs[j] = segRef{lsn: seg.LSN, off: pos.off, length: pos.length}
							continue
						default:
							if bound {
								// ★一次性 fencing 特赦: 仅当 env 武装的 LSN 命中此点,
								// 死代(上代缓冲尾)让位于恢复后真历史 —— 审计双内容 FNV
								// 与分叉点, 走下方重写路径(绑定时刻按新长度统一入账,
								// 旧代台账由 commit 循环清账, 与常规重写完全同路)。
								yield := fencingOverrideAll ||
									(fencingOverrideLSN != 0 && fencingOverrideLSN == seg.LSN) ||
									!fencingKeepOld.Load()
								if yield {
									if s.opts.Logger != nil {
										s.opts.Logger.Printf("[WARN] filestore: ★死代让位 lsn=%d: 旧 %d 字节(fnv=%016x) → 新 %d 字节(fnv=%016x), firstDiff=%d(epoch-fencing 默认语义; 旧记录留在追加区可取证)", seg.LSN, len(existing), fnv1a64(existing), len(seg.Payload), fnv1a64(seg.Payload), diff)
									}
								} else {
									if s.opts.Logger != nil {
										s.opts.Logger.Printf("[ERROR] filestore: lsn=%d 已绑定但内容异变 —— fencing 违例, 保留已提交字节 (existLen=%d newLen=%d firstDiff=%d newBase=%d)", seg.LSN, len(existing), len(seg.Payload), diff, base)
									}
									srefs[j] = segRef{lsn: seg.LSN, off: pos.off, length: pos.length}
									continue
								}
							}
							if s.opts.Logger != nil {
								s.opts.Logger.Printf("[WARN] filestore: lsn=%d 未绑定异字节重写(故障切换修复) (existLen=%d newLen=%d firstDiff=%d)", seg.LSN, len(existing), len(seg.Payload), diff)
							}
							// fallthrough to rewrite below
						}
					}
					// rerr != nil (存在映射但读不出): 视为缺失, 重写。
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
	// redo 侧仅在非空时补齐(空批不该凭空占 512B); meta 侧恒补齐 —— 每次批次的
	// 起止都必须落在 512 边界上, 否则下一条记录的 recLen 会读歪。
	if pad := align512(int64(redoBuf.Len())) - int64(redoBuf.Len()); redoBuf.Len() > 0 && pad > 0 {
		redoBuf.Write(make([]byte, pad))
	}
	if pad := align512(int64(metaBuf.Len())) - int64(metaBuf.Len()); pad > 0 {
		metaBuf.Write(make([]byte, pad))
	}
	redoBytes, metaBytes := redoBuf.Bytes(), metaBuf.Bytes()
	// O_DIRECT 还要求缓冲区**起始地址**对齐: 长度已由批尾填充保证, 这里补地址对齐。
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
		// io_uring 不可用/失败时静默降级为经典 WriteAt + 屏障, 不重试也不报错;
		// 实际用上了多少次由 s.ioringOK 计数暴露(探测证据)。
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
			// J5 泄漏修复(9.24): 绑定 = 直发记录的终点事件 —— 整条清账(同
			// LSN 多代预留一次清零, 之后同 LSN 的重发/重绑定全部 no-op)。
			// raft 原生段的台账天然为空, drain 同样 no-op, 计数继续按
			// 记录全长累计(bound 口径与之前一致)。
			s.unboundDrain(sr.lsn)
			// J5: 绑定时刻 = meta 首次建 lsnIndex 引用。字节数按记录全长计
			// (与 unboundBytes 预留口径一致)。
			s.boundRedoSegs++
			s.boundRedoBytes += int64(sr.length)
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

// splitEntry 决定一条日志的 redo 段划分: 多段 codec(RedoSegmentCodec)优先,
// 普通 codec 至多给 1 段(lsn==0 视为「非 redo 条目」), 无 codec 则无段。
//
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

// mergeEntry 按 segLSNs 从 redo.log 读回各段并交给 codec 合并, 重建一条 redo
// 条目的 Data。调用方不持任何锁。
//
// mergeEntry rebuilds Data for a redo entry from its segments.
// 两阶段读锁分离(12.14): 位置+fd 抓拍在 RLock 内, 盘读/合并全部锁外
// (redo.log append-only 不可变; redoIoMu.RLock 钉住 fd 防压实换刀时
// 被 close)。调用方不持任何锁。
//
// 算法解读：
//
//	原理：三段式 —— pinRefReadLocked 在 mu.RLock 内一次性抓拍 (fd, 各段 off/len)
//	并完成到 redoIoMu.RLock 的持链移交; 随后全部 ReadAt + Unframe 在锁外做;
//	最后按 codec 能力合并(多段走 RedoSegmentCodec.MergeSegments, 单段可退化为
//	Codec.Merge 或直接返回裸 payload)。
//	关键不变量：① 抓拍到的 (off,len) 在读取期间必须仍然有效 —— 依赖 redo.log
//	append-only 与压实「只搬已发布记录且新文件逐字节等价」; ② fd 在整个读期间
//	不能被 close, 由 redoIoMu.RLock 保证(压实换刀持 redoIoMu.Lock); ③ 合并顺序
//	必须与写入顺序一致(segs 按 LSN 列表序), 否则业务信封错位。
//	代价与边界：段数 N 时是 N 次随机读(每段可能一次 O_DIRECT 对齐读, 放大到
//	512 边界); 段缺失/读失败直接向上抛, 由调用方(GetLog/ResolveElidedData)
//	转成 ErrLogNotFound 或 RedoDataMissingError。
//	设计权衡：把「抓拍」与「读盘」分成两段是为了不让慢 I/O 占住全局 mu —— 早期
//	实现整段持 mu, 导致 redo 读延迟直接串住 StoreLogs。代价是引入 redoIoMu 与
//	锁序约定(mu → redoIoMu), 以及「压实期间读者仍能继续读旧 fd」这一必须成立
//	的前提。
func (s *Store) mergeEntry(base uint64, segLSNs []uint64) ([]byte, error) {
	s.mu.RLock()
	f, poss, err := s.pinRefReadLocked(segLSNs)
	if err != nil {
		return nil, err // pinRefReadLocked 失败路径已放 mu
	}
	defer s.redoIoMu.RUnlock()
	return s.mergeCaptured(f, base, segLSNs, poss)
}

// captureRefReadLocked 在 mu(读或写)持有期间抓拍 segLSNs 的落点与 redoF
// 身份 —— 同一临界区完成, 与 DeleteRange/CompactRedo 无 TOCTOU 窗口。
func (s *Store) captureRefReadLocked(segLSNs []uint64) (*os.File, []redoPos, error) {
	if s.redoF == nil {
		return nil, nil, raft.ErrLogNotFound
	}
	poss := make([]redoPos, len(segLSNs))
	for j, lsn := range segLSNs {
		pos, ok := s.redoMap[lsn]
		if !ok {
			return nil, nil, raft.ErrLogNotFound
		}
		poss[j] = pos
	}
	return s.redoF, poss, nil
}

// pinRefReadLocked: 调用方持 mu.RLock; 成功返回时【mu 已放、redoIoMu.RLock
// 已持】(调用方负责 redoIoMu.RUnlock)。必须持链移交(mu.RLock → redoIoMu.RLock
// 不松手交接): 压实换刀需 mu.Lock+redoIoMu.Lock, 若先放 mu 再取 redoIoMu,
// 压实在窗口内完成"换 fd+关旧 fd", 读者随后对已关闭 fd ReadAt(实测 EBADF)。
// 失败路径自行放 mu 后返回。
//
// 算法解读：
//
//	原理：两把锁「不松手交接」—— 先在 mu.RLock 下取 redoIoMu.RLock(此时仍持 mu),
//	拿到 redoIoMu 之后才放 mu, 把「抓拍位置」与「钉住 fd」两步做成原子的一次交接。
//	关键不变量：整个函数返回时, 调用方持有的唯一锁是 redoIoMu.RLock, 而 fd 一定
//	与抓拍时的 s.redoF 相同。两把锁缺一不可: 只有 mu 则盘读期间仍阻塞写路径;
//	只有 redoIoMu 则抓拍 (off,len) 时可能与 StoreLogs/DeleteRange 竞态。若先放 mu
//	再取 redoIoMu, 中间窗口里 CompactRedo 可以完成「换 fd + 关旧 fd」, 读者随后对
//	已关闭的 fd ReadAt 会拿到 EBADF(实测捕获)。
//	代价与边界：持链移交期间 mu 与 redoIoMu 短暂同时被持, 压实(需 mu.Lock +
//	redoIoMu.Lock)会在这两个点之间阻塞 —— 但那是后台路径, 换来读路径不再占全局锁。
//	设计权衡：为什么不用一把锁 + 引用计数 fd? 那样每次读都要改共享计数、且 fd 生命
//	周期管理复杂; 用读写锁把「身份钉住」这件事显式化, 代价只是一条固定的锁序约定。
func (s *Store) pinRefReadLocked(segLSNs []uint64) (*os.File, []redoPos, error) {
	f, poss, err := s.captureRefReadLocked(segLSNs)
	if err != nil {
		s.mu.RUnlock()
		return nil, nil, err
	}
	s.redoIoMu.RLock()
	s.mu.RUnlock()
	return f, poss, nil
}

// mergeCaptured 在 redoIoMu.RLock 已持(钉住 fd)的前提下执行: 逐段 ReadAt +
// Unframe, 再由 codec 合并为业务信封。全程不碰 mu。
func (s *Store) mergeCaptured(f *os.File, base uint64, segLSNs []uint64, poss []redoPos) ([]byte, error) {
	segs := make([]RedoSegment, len(segLSNs))
	for j, lsn := range segLSNs {
		payload, err := s.readRedoAt(f, poss[j])
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

// reloadRef 以 redo.log 为数据权威重建内存视图: 先扫 redo 建 lsn→pos 与 good
// tail, 再扫 meta, 丢弃「引用到已被撕走字节」的 meta 记录(在该点截断)。
//
// reloadRef scans redo.log (data authority) then raft.log (meta), dropping
// meta entries whose redo bytes were torn away.
//
// #163 W4 行级根修: redo 扫描现在容忍 512 对齐零洞(WriteRedoDirect 二阶段
// 失败回滚留下的占位区间, 可能先于并发已发布记录落位)。旧实现遇洞即
// errScanStop → redoGood 缩短 → 引用洞后字节的已提交 meta 全部误判为
// "redo 被撕走" → truncateAtGapLocked 级联截断 → 已提交条目不可达 →
// commit=0 死锁。现洞后出现 CRC 合法记录则继续收集, 洞只是被跳过。
// 未被任何 meta 引用的洞(孤零洞)由 CompactRedo 顺带收走。
//
// 算法解读：
//
//	原理：两趟扫描。第一趟用 scanRecords 解 redo 记录, 建 redoMap[lsn]→(off,len)
//	并把 CRC 合法的记录延伸到 good tail(中间允许 512 对齐零洞, 洞只是被跳过);
//	第二趟扫 meta, 对每条 redo 条目检查其全部段是否都落在 good tail 之内 ——
//	任何一个越界就 errScanStop, 之后的 meta 全部丢弃。最后把两处 good tail 记为
//	endOff/redoEnd, 再调 truncateAtGapLocked 处理**索引空洞**(在首个 index 不连续
//	处截断 meta 并清掉其上的 lsnIndex), 并从 redoMap 反推未绑定台账。
//	关键不变量：① redo.log 是权威 —— meta 说「有」不算数, 必须 redo 里真的有
//	且完整; ② meta 侧仍是「一遇坏记录即截断」的前缀语义(不做洞后恢复), 而 redo
//	侧允许 512 对齐零洞被跳过 —— 两侧容忍度刻意不对称, 见下方 #163 说明;
//	③ lsnIndex 是「已绑定」的判据, 未进 lsnIndex 的 redoMap 项 = 直发残留, 必须
//	重新挂账(unboundPending/unboundBytes), 否则水位记账跨重启失真。
//	代价与边界：洞容忍让扫描不能「一遇零就停」, 需要识别「零洞后仍有合法 CRC
//	记录」; 若洞后确实是垃圾, 仍在第一个坏记录处截断。台账不跨进程持久化, 靠
//	本函数从 redoMap 重建(等价现金额), 因此重建的正确性直接决定背压是否误触发。
//	设计权衡：旧实现「遇洞即停」会把洞后被引用的已提交 meta 全判为「redo 被撕走」
//	并级联截断, 已提交条目不可达 → commit=0 死锁(#163)。故宁可容忍零洞、把
//	「未绑定直发残留」的清理交给 CompactRedo, 也不在重载路径上做激进截断。
func (s *Store) reloadRef(rawMeta, rawRedo []byte) error {
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	// 1. redo scan: build the lsn→(off,len) presence map, find the good tail.
	redoGood := int64(fileHeaderLen)
	if len(rawRedo) >= fileHeaderLen {
		var redoStats scanStats
		redoGood, redoStats, _ = scanRecords(rawRedo, fileHeaderLen, true, func(off int64, body []byte) error {
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
		if redoStats.holes > 0 {
			s.redoHealedHoles += uint64(redoStats.holes)
			s.redoHoleBytes += redoStats.holesBytes
			if s.opts.Logger != nil {
				s.opts.Logger.Printf("[WARN] filestore: %s redo 重载越过 %d 个 512 对齐零洞(%d B) —— #163 孔洞容忍(WriteRedoDirect 回滚残留), 记录完好不截断",
					s.path, redoStats.holes, redoStats.holesBytes)
			}
		}
	}
	// 2. meta scan: rebuild logs; drop entries referencing torn redo bytes.
	metaGood, _, err := scanRecords(rawMeta, fileHeaderLen, true, func(off int64, body []byte) error {
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
	// J5 泄漏修复(9.24): 台账随进程存亡 —— 重启后从 redo 扫描重建: redoMap
	// 里未被任何 meta 引用(lsnIndex 无)的 lsn 即未绑定直发残留, 重新挂账,
	// 现金恢复等价水位; 台账不跨进程持久化(现金额随 map 重建, 无泄漏窗口)。
	if len(s.redoMap) > 0 {
		s.unboundPending = make(map[uint64]int64)
		s.unboundBytes = 0
		for lsn, pos := range s.redoMap {
			if _, bound := s.lsnIndex[lsn]; !bound {
				s.unboundPending[lsn] = int64(pos.length)
				s.unboundBytes += int64(pos.length)
			}
		}
	}
	return nil
}

// readRedoPayload 经 presence map(redoMap)读回某个 lsn 的 payload 字节。
//
// readRedoPayload returns the payload bytes for an lsn via the presence map.
// 调用方必须持有 mu(写锁)—— 供 storeLogsRef/WriteRedoDirect 的去重判定用
// (写锁本身已排除压实换刀, fd 稳定)。锁外读盘场景走 mergeCaptured/ReadRedo。
func (s *Store) readRedoPayload(lsn uint64) ([]byte, error) {
	pos, ok := s.redoMap[lsn]
	if !ok {
		return nil, raft.ErrLogNotFound
	}
	return s.readRedoAt(s.redoF, pos)
}

// readRedoAt 纯 I/O 半部: 从指定 fd 的 (off,len) 读记录并 Unframe。
// 不带任何锁状态访问(s.opts/s.directIO 打开后不可变); fd 稳定性由调用方
// 保证(mu.Lock 持有者, 或 redoIoMu.RLock 钉住的锁外读者)。
func (s *Store) readRedoAt(f *os.File, pos redoPos) ([]byte, error) {
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	var raw []byte
	if s.directIO {
		r, err := readAtAligned(f, pos.off, pos.length)
		if err != nil {
			return nil, err
		}
		raw = r
	} else {
		raw = make([]byte, pos.length)
		if _, err := f.ReadAt(raw, pos.off); err != nil {
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

// redoBlockHeaderLen: InnoDB redo 512B 块的头部长度(LOG_BLOCK_HDR_SIZE=12)。
// 块头 data_len(偏移 4-5)随未满块填充推进 —— 同 start LSN 的两个快照在头区
// 分叉是常态(直发快照早于绑定快照), 去重判定把头区视作可演化元数据。
const redoBlockHeaderLen = 12

// firstDiffAt 返回两个字节串首个不同字节的偏移; 前缀相等但长度不同返回较短长度;
// 完全相等返回 -1。供 fencing/快照更新诊断日志与判定使用。
// fencingOverrideLSN: 一次性 fencing 特赦的目标 LSN(0=未武装)。
// 由 Open 经 env LOGDB_FILESTORE_FENCING_OVERRIDE_LSN 武装; 语义见 filestore.go。
var fencingOverrideLSN uint64

// fencingOverrideAll: 临时手术模式(env LOGDB_FILESTORE_FENCING_OVERRIDE_ALL)——
// 任意 LSN 的绑定异变都让位于当前代(全量 FNV 审计)。仅排雷期武装。
var fencingOverrideAll bool

// fencingKeepOld: 旧行为保险丝(env LOGDB_FILESTORE_FENCING_KEEP_OLD=1)——
// 绑定异变时冻结保留旧字节(2026-10-09 前的默认)。默认关 = epoch 感知让位:
// 能过 RPC 层 writer_epoch 闸的写皆是当前代, 绑定层撞见的分叉必为死代缓冲尾
// (同代重写经 fnv 实证恒字节一致), 死代让位 + FNV 审计, 旧记录仍在追加区
// 可取证。特赦 env(OVERRIDE_LSN/_ALL)仍兼容(强制让位), 优先级高于保险丝。
var fencingKeepOld atomic.Bool

// fnv1a64: 审计用内容指纹(FNV-1a 64)。
func fnv1a64(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// firstDiffAt 返回 a、b 首个不同字节的偏移; 若一方是另一方的前缀则返回较短者
// 的长度; 完全相等返回 -1。供 fencing / 快照更新判定与诊断日志使用。
//
// 算法解读：
//
//	原理：线性扫描到第一个不同字节即返回其下标; 扫完公共长度仍相同, 则用长度差
//	区分「前缀关系」(返回较短长度)与「完全相等」(返回 -1)。
//	关键不变量：返回值必须能同时表达三种情形 —— -1 = 逐字节相同; == len(较短者)
//	= 一方是另一方的前缀; 其它 = 真正的内容分叉。调用方(storeLogsRef /
//	WriteRedoDirect)正是按「diff == len(existing)」「diff < redoBlockHeaderLen」
//	这两条判据把「块渐满扩展」「块头 data_len 推进」与「真分叉」分开的 —— 所以
//	返回 -1 而不是 0 来代表「相同」是语义的一部分, 不能省。
//	代价与边界：O(min(len)) 字节比较, 最坏情形(前缀相等但长度不同)才走满全程;
//	对「同 LSN 但块头演化」的记录, 差异通常落在头 12B 内, 几乎立即返回。
//	设计权衡：用「同 LSN 同前缀」而非哈希/全等来判断去重, 是因为 InnoDB log-writer
//	对未满 512B 块会随数据渐满重写同 start 段, 块头 data_len 会推进 —— 逐字节全等
//	会把这种正常演化误判成脑裂。故先给「差在哪儿」一个精确答案, 再由调用方按
//	块头长度阈值分类, 而不是在这里直接下「相同/不同」的二元结论。
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

// compactSkipCount 统计「keepFrom 被 clamp 到最小存活 LSN ⇒ 无可丢 ⇒ 跳过重写」的次数。
// 仅用于日志节流(见 CompactRedo), 不参与任何判定。
var compactSkipCount uint64

// CompactRedo 按检查点水位压实 redo.log: 把 lsn >= keepFromLSN 的存活记录重写
// 进一个新文件再 rename 覆盖, 丢弃更老的记录。keepFromLSN 会被 clamp 到「仍被
// 存活 meta 引用的最小 LSN」, 因此已提交且被引用的 redo 永不被丢。必须由业务
// 检查点驱动(raft 快照先覆盖被丢区间)。全程持 s.mu 写锁。
//
// CompactRedo drops redo records with lsn < keepFromLSN by rewriting the
// survivors to a fresh file and renaming it over redo.log. keepFrom is
// clamped to the smallest lsn still referenced by live meta entries, so
// committed-and-referenced redo is never dropped. Callers must drive this
// from the business checkpoint (raft log snapshot must cover the dropped
// range first).
//
// 算法解读：
//
//	原理：先按存活 meta 把所有段 LSN 取最小值 clamp 到 keepFromLSN; 若无任何
//	记录可丢(keepFromLSN <= redoMap 最小 LSN)则**直接返回不重写**。否则把
//	redoMap 的 LSN 排序, 逐条读旧文件、CRC 校验、按序写入 .tmp(每满 1MiB 落一次),
//	Sync + Close + rename + syncDir 之后, 在 mu+redoIoMu 双锁下换刀: 换 redoF、
//	换 redoMap、清掉被丢 LSN 的未绑定台账, 最后关旧 fd。
//	关键不变量：① 被丢的 LSN 必须已不被任何存活 meta 引用(clamp 保证); ② 新文件
//	内容与旧文件对幸存者逐字节等价, 故换刀期间仍持旧 fd 的读者读到的是同一份字节;
//	③ 换刀必须在 redoIoMu.Lock 下做 —— 要等所有锁外读者的 in-flight ReadAt 收工
//	才能关旧 fd; ④ 顺序固定为「写 .tmp → Sync → rename → 重新打开 → 换刀」, rename
//	之前失败时 redo.log 完好无损。注意 rename 之后、换刀之前若 OpenFile 失败, 磁盘上
//	已是新文件而 s.redoF 仍指向被替换掉的旧 inode —— 此处只返回错误, 不改变状态。
//	代价与边界：重写体积 ∝ 存活记录总量, 与「能丢多少」无关 —— 这正是它退化成
//	写放大器的边界: 当 raft 日志因快照滞后而长期不截断时, clamp 会让 keepFromLSN
//	恒等于最小 LSN, 于是「无可丢」早退生效(实测曾每 ~20s 完整重写 832MiB 并全程
//	持 mu, 把 StoreLogs 串到建表塌速 0.5 表/s, 见下方 #105 修复注释)。
//	设计权衡：为什么不在 DeleteRange 里顺手回收 redo 空间? 因为 LSN 在 index 序上
//	不单调(批提交会折叠不连续段), raft 冲突截断无法安全映射到 redo 区间; 故 redo
//	回收只认业务检查点水位。为什么不做增量/后台回收? 那需要在写路径之外维护一份
//	空洞账, 复杂度远高于收益 —— 在当前口径下「无可丢就不写」已消掉绝大部分开销。
func (s *Store) CompactRedo(keepFromLSN uint64) error {
	// ★#105 诊断(2026-09-25): 先**只测量**, 不改语义。
	// 待证假设: 压实持 s.mu.Lock() 横跨「读旧→校验→写新→Sync→Rename→syncDir」全过程
	// (见下方换刀临界区注释), 而重写体积 ∝ len(redoMap)、redoMap 又随检查点滞后增长
	// ⇒ StoreLogs 被串住 ⇒ 建表塌速。这条假设此前**未被证明**, 所以先打点再动手。
	// 两个数分开打: wait=等锁(别人占着, 直接反映对写路径的阻塞),
	// run=本次重写(含 Sync), 两者一起才能区分「锁被谁占」和「占多久」。
	tEnter := time.Now()
	s.mu.Lock()
	waited := time.Since(tEnter)
	defer s.mu.Unlock()
	// ★len(s.redoMap) 必须在**取到锁之后**读: 它在 StoreLogs/WriteRedoDirect 里被写,
	// 锁外读是数据竞争(-race 实测: refstore.go:699 对 refstore.go:387/direct.go:175
	// 的 mapassign)。取到锁后读到的才是「本次重写要遍历的那个 map」的大小 ——
	// 也正是打点想量的那个数(等锁那段时间里 map 长没长, 与本次重写体积无关)。
	redoMapLenAtEntry := len(s.redoMap)
	// 注意 defer 是后进先出: 本函数在 s.mu.Unlock() **之前**执行, 仍在锁内。
	// 只读局部变量 + 一次 stderr 写, 且仅在超阈值时发生, 不改变热路径。
	defer func() {
		if d := time.Since(tEnter); d >= 100*time.Millisecond {
			fmt.Fprintf(os.Stderr,
				"[#105][compact] CompactRedo 慢: 总耗时=%v 等锁=%v redoMap=%d 条 keepFrom=%d\n",
				d.Round(time.Millisecond), waited.Round(time.Millisecond),
				redoMapLenAtEntry, keepFromLSN)
		}
	}()
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
	// ★#105 修复(2026-09-25): 「无可丢 ⇒ 直接返回, 不重写」。
	//
	// 上面那个 clamp 把 keepFromLSN 压到**现存最老 raft 条目所引用的 LSN**。当 raft
	// 日志因快照滞后而不被截断时(本档实测: FSM 快照停在 index=10298, commit=18765),
	// 这个 LSN 恒等于 redoMap 的最小 LSN ⇒ 下面的重写会**逐字节复制整个文件、一条都不丢**。
	// 实测(logdb-tps 41k 表档, 2026-09-25 20:15):
	//   redo.log 恒 872415232B = 832MiB, 每 ~20s 被完整重写一次;
	//   tmp 文件实测长到 752MB / 870MB 才 rename(即整文件重写而非部分);
	//   占 1.8T 数据盘的 40%, 全程持 s.mu.Lock()(含 nf.Sync()) ⇒ StoreLogs 被串住;
	//   调用侧读数 storeq/storew=12987ms、commit_t=18091ms, 建表塌到 0.5 表/s。
	// 重写的**语义目标**是「丢掉 < keepFrom 的记录」; 没有可丢的 ⇒ 目标已达成 ⇒ 不该写。
	//
	// 刻意**不**顺带回收「未被 redoMap 引用的字节」(空洞/陈旧段): 那正是需要持锁做
	// 全文件重写的那部分, 而在 keepFrom 被 clamp 到最小 LSN 的前提下, 它们对应的空间
	// 收益远小于「整文件重写 + fsync」的代价。要回收它们应该走增量/后台路径, 而不是
	// 让写路径在每一次检查点上都付一遍 832MiB 的账。
	minLSN, have := uint64(0), false
	for lsn := range s.redoMap {
		if !have || lsn < minLSN {
			minLSN, have = lsn, true
		}
	}
	if !have || keepFromLSN <= minLSN {
		if n := atomic.AddUint64(&compactSkipCount, 1); n == 1 || n%1000 == 0 {
			fmt.Fprintf(os.Stderr,
				"[#105][compact] 跳过重写(无可丢): keepFrom=%d redoMap=%d 条 minLSN=%d 第%d次\n",
				keepFromLSN, len(s.redoMap), minLSN, n)
		}
		return nil
	}
	framer := s.opts.Framer
	if framer == nil {
		framer = StdFramer{}
	}
	dir := filepath.Dir(s.path)
	// 固定的 .tmp 名: 压实全程持 s.mu, 同一 store 上不会有两个压实抢同一个 tmp。
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
		// 1MiB 攒批: 太小则 write syscall 次数多, 太大则一次向 .tmp 吐出过多脏页。
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
	// 必须先 Sync 新文件再 rename: 否则掉电后可能 rename 出一个内容不全的 redo.log。
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
	// 换刀临界区(mu.Lock + redoIoMu.Lock, 锁序一致): 等锁外读者在旧 fd 上的
	// in-flight ReadAt 全部收工后再关旧 fd —— 抓拍 (fd,off,len) 的读者读的
	// 是 append-only 不可变记录, 旧 inode 内容与新文件逐字节等价, 关 fd 才需互斥。
	s.redoIoMu.Lock()
	oldF := s.redoF
	s.redoF = newF
	s.redoMap = newMap
	s.redoEnd = writeOff
	// J5 泄漏修复(9.24): 被压实丢弃的 lsn(< keepFrom, 盘上字节已消失)整条
	// 清账 —— 否则未绑定直发记录被 GC 后残账钉住水位。幸存者保留台账继续
	// 等绑定(其字节已搬入新文件)。
	for lsn, p := range s.unboundPending {
		if lsn < keepFromLSN {
			delete(s.unboundPending, lsn)
			s.unboundBytes -= p
		}
	}
	if s.unboundBytes < 0 {
		s.unboundBytes = 0
	}
	_ = oldF.Close()
	s.redoIoMu.Unlock()
	return nil
}

// syncDir 对目录本身做一次 fsync —— rename 的持久化依赖目录项落盘, 只 Sync 文件
// 数据不够(文件在, 但「新名字指向它」这件事可能还在页缓存里)。
// 打开或同步失败一律静默忽略: 这是尽力而为的加固, 不是正确性依赖。
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// sortU64 就地把 uint64 切片按升序插入排序。压实重写前用它把 redoMap 的 LSN
// 排成有序, 使新文件里的记录按 LSN 单调(便于扫描与定位)。
// 注意是 O(n²): 只用在压实路径, 不在写/读热路径上。
func sortU64(a []uint64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// ---------------------------------------------------------------------------
// Alignment helpers (O_DIRECT tier).

// alignedCopy 返回 b 的一份 512 对齐副本(O_DIRECT 写用)。长度与偏移此前已由
// 批尾填充对齐, 这里只补「缓冲区起始地址」这一项对齐要求。
//
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

// alignedBytes 分配一块「起始地址 512 对齐、长度恰为 n」的字节切片。
// 做法: 多要 511B, 再把首地址向上取整到 512, 返回原切片的子切片 —— 代价是
// 这块底层数组比 n 略大, 且子切片会让整个底层数组一直存活(不可 GC 前半段)。
func alignedBytes(n int) []byte {
	raw := make([]byte, n+511)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	pad := int((512 - (base & 511)) & 511)
	return raw[pad : pad+n]
}

// readAtAligned 在 O_DIRECT fd 上服务任意 (off,len) 读: 把区间向外扩到 512 边界
// 再读, 最后裁回调用方要的窗口。
//
// readAtAligned services an arbitrary (off,len) read from an O_DIRECT fd.
//
// 算法解读：
//
//	原理：O_DIRECT 要求偏移、长度、缓冲区地址三者都按块(512)对齐, 所以不能直接
//	读 [off, off+n)。做法是 lo = off 向下取整到 512, hi = off+n 向上取整到 512,
//	分配一块地址也对齐的 hi-lo 字节缓冲, 一次 ReadAt(buf, lo), 再切片裁回
//	buf[off-lo : off-lo+n] 返回给调用方 —— 扩读进来的前导/尾随字节对调用方不可见。
//	关键不变量：① 三个对齐要求必须同时满足(只对齐长度或只对齐偏移都会被内核
//	EINVAL 拒掉); ② 返回切片与原缓冲区共享底层数组, 生命周期绑定, 调用方不得
//	假设它是独立副本; ③ 「短读」才是失败 —— 读到的字节数不足 off-lo+n 时返回
//	io.ErrUnexpectedEOF, 而 io.EOF 本身不算错(恰好读到文件尾是合法的)。
//	代价与边界：单次读最多放大到 1KiB 额外字节(前导 <512 + 尾随 <512); 若 off 或
//	n 未按 512 对齐(redo 记录本身就是这样, 记录之间没有 512 填充), 扩读就是常态
//	而非例外。文件被并发截断时可能在扩读区拿不到数据, 表现为 ErrUnexpectedEOF。
//	设计权衡：为什么不在写入时就把每条记录都补到 512 从而让读取天然对齐? 那会让
//	redo.log 体积膨胀数倍(redo 段往往只有几十到几百字节), 空间代价远大于读放大。
//	故选择「写紧凑 + 读时扩窗」。
func readAtAligned(f *os.File, off int64, n int) ([]byte, error) {
	lo := off &^ 511               // 向下取整到 512 边界(O_DIRECT 的偏移约束)
	hi := align512(off + int64(n)) // 向上取整, 保证覆盖 [off, off+n) 全部字节
	buf := alignedBytes(int(hi - lo))
	got, err := f.ReadAt(buf, lo)
	// io.EOF 不算错: 只有「读到的字节数不足」才是真问题(见下方长度检查)。
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int64(got) < off-lo+int64(n) {
		return nil, io.ErrUnexpectedEOF
	}
	return buf[off-lo : off-lo+int64(n)], nil
}

// openDirectFlag 在 on 时返回 unix.O_DIRECT, 否则返回 0(即不附加任何打开标志)。
// 供 os.OpenFile 的 flag 位或运算使用。
func openDirectFlag(on bool) int {
	if on {
		return unix.O_DIRECT
	}
	return 0
}

// timeFromUnixNano 把 unixnano 时间戳还原成 time.Time, 与 meta 头的 appendedAt
// 字段配套(写入侧用 UnixNano, 0 表示「未设置」故调用方会先判零)。
func timeFromUnixNano(ns int64) time.Time { return time.Unix(0, ns) }
