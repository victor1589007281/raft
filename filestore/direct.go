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

// WriteRedoDirect 把一段 redo 段直接写进 redo.log 而不经过 raft(P3 数据面):
// 返回即持久(自带屏障), 同 LSN 重复投递幂等; 该记录在「已提交的 raft meta 引用
// 它」之前都是**未绑定**的, 读者必须以已提交前沿为闸。
//
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
//
// 算法解读：
//
//	原理：三阶段 + 换刀重试。阶段 1(mu 写锁): 校验模式、做与 storeLogsRef 同构的
//	流快照去重判定、检查未绑定水位上限、推进 s.redoEnd 占位(并发直发各自拿到互异
//	区间)、登记 per-LSN 预留台账, 然后在**仍持 mu 时**取 redoIoMu.RLock 完成持链
//	移交并放开 mu。阶段 2(仅 redoIoMu.RLock): 写盘 + 屏障(io_uring 链或 WriteAt +
//	barrierOn), 全程不碰 mu。阶段 3(mu 写锁): 校验 redoF 仍是阶段 1 那个 fd, 是则
//	发布 redoMap 与台账, 否则释放预留、以新 fd 重走全流程(最多 3 次)。
//	关键不变量：① 「占位即预留」—— s.redoEnd 在锁内推进, 保证并发直发写的是互不
//	重叠的区间, 这是锁外写盘能安全并行的前提; ② 阶段 2 必须持 redoIoMu.RLock 才能
//	写 —— 否则压实换刀可能在 WriteAt 之前 close 掉这个 fd(EBADF, -race 烤机实测
//	抓到过); ③ 发布与占位必须成对: 失败路径要 unboundRelease 退回预留, 换刀路径
//	要 unboundDrain 整条清账, 否则水位只涨不落; ④ 幂等: 同 LSN 同字节的重复投递
//	在阶段 1 就返回 nil, 不产生第二次写。
//	代价与边界：直发写盘与 StoreLogs 可以真并行, 但代价是引入「未绑定字节」这一
//	中间态 —— 必须有 UnboundRedoLimit 背压兜底, 超限返回 ErrUnboundFull 让调用方
//	降级回 raft 路径; 压实若在阶段 1 与阶段 3 之间完成换刀, 本次记录会留在旧 inode
//	(新文件只搬已发布记录), 故必须重试而非硬失败。
//	设计权衡：为什么不让直发也持 mu 写盘? 高直发速率下 mu 被打满, follower 的
//	AppendEntries StoreLogs 排不上队 → 提交往返膨胀 → 计算层流水线截止重试 →
//	自激 lurch(#19)。为什么敢在锁外做 fdatasync? 因为 append-only 文件上对互异
//	偏移的 fdatasync 是幂等的 —— 它刷的是「已写入的脏页」, 不改变任何记录的语义。
func (s *Store) WriteRedoDirect(base, lsn uint64, payload []byte) error {
	// 压实换刀重试: 占位→锁外写→发布三阶段之间若 CompactRedo 换了 fd,
	// 我的未发布记录不会被带进新文件(压实只搬 redoMap 已发布者) ——
	// 发布前校验 fd 身份, 变了就以新 fd 重走全流程(去重判定在新 map 上重做)。
	for attempt := 0; ; attempt++ {
		// 3 次上限: 压实换刀是低频事件, 连续三次都撞上说明直发正被压实长期
		// 压制, 与其继续空转不如交回 raft 路径承载。
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
					// 旧快照迟到(更短/同长): 保留已存的更新版本。直发重发被
					// 吸收 —— 本次的预留不发生(在下方记账点跳过), 既有台账不动。
					s.mu.Unlock()
					return nil
				case len(payload) > len(existing) && (diff == len(existing) || diff < redoBlockHeaderLen):
					// 更新快照: 落到下面重写。旧代预留先整条清账(被新记录
					// 替代 —— 多代 v1→v2→v3 各自挂账, 若只靠绑定侧释放
					// pos.length 会留下旧代残账, K6 实测的 60% 泄漏主体);
					// 新代预留由下方记账点登记。
					s.unboundDrain(lsn)
				default:
					if _, bound := s.lsnIndex[lsn]; bound {
						// ★epoch 感知 fencing(默认让位; KEEP_OLD env 可回旧行为): 死代缓冲尾
						// 让位于恢复后真历史 —— 审计双内容 FNV, 落下方未绑定重写(先整条清账)。
						// 直发是 ⑩c 灌洞的主力路径, 洞解在此。
						if fencingOverrideAll || (fencingOverrideLSN != 0 && fencingOverrideLSN == lsn) || !fencingKeepOld.Load() {
							if s.opts.Logger != nil {
								s.opts.Logger.Printf("[WARN] filestore: ★direct fencing 特赦生效 lsn=%d: 死代 %d 字节(fnv=%016x) 让位真历史 %d 字节(fnv=%016x), firstDiff=%d", lsn, len(existing), fnv1a64(existing), len(payload), fnv1a64(payload), diff)
							}
							s.unboundDrain(lsn)
						} else {
							s.mu.Unlock()
							return fmt.Errorf("filestore: direct lsn=%d 与已绑定内容真分叉(firstDiff=%d existLen=%d newLen=%d) —— fencing", lsn, diff, len(existing), len(payload))
						}
					}
					if s.opts.Logger != nil {
						s.opts.Logger.Printf("[WARN] filestore: direct lsn=%d 未绑定异字节重写(故障切换修复) (existLen=%d newLen=%d firstDiff=%d)", lsn, len(existing), len(payload), diff)
					}
					// 未绑定异字节重写: 同样整条清账再重写(旧记录被替代)。
					s.unboundDrain(lsn)
				}
			}
		}
		framer := s.opts.Framer
		if framer == nil {
			framer = StdFramer{}
		}
		body := framer.Frame(base, lsn, payload)
		recLen := 4 + len(body)
		// 背压: 未绑定字节超限即拒。调用方收到 ErrUnboundFull 应停直发、回退
		// raft 路径, 直到绑定(meta 建引用)把积压消费掉。0 = 不限。
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
		// 补到 512 边界后才推进 redoEnd —— 维持「批次起止落在 512 边界」不变量。
		s.redoEnd = off + align512(int64(len(buf)))
		// J5 泄漏修复(9.24): 预留登记台账(per-LSN), 绑定/重写按账释放。
		s.unboundReserve(lsn, int64(recLen))
		f := s.redoF
		// 持链移交(与 pinRefReadLocked 同构): 仍持 mu 时取 redoIoMu.RLock ——
		// 压实换刀需 mu.Lock+redoIoMu.Lock, 此移交令换刀绝不可能在我的
		// WriteAt 前完成 close(否则会 EBADF —— -race 烤机实测抓获)。
		s.redoIoMu.RLock()
		s.mu.Unlock()

		rollbackReservation := func() {
			s.mu.Lock()
			s.unboundRelease(lsn, int64(recLen))
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
			s.unboundRelease(lsn, int64(recLen))
			s.mu.Unlock()
			continue // 压实把未发布记录留在旧 inode —— 必须以新 fd 重写
		}
		if usedIORing {
			s.ioringOK++
		}
		if s.redoMap == nil {
			s.redoMap = make(map[uint64]redoPos)
		}
		// 发布即终点切换: 若同 LSN 在我阶段1清账后又有并发直发先发布(我的
		// 预留+它的台账并存), 我这条是旧记录被替代 —— 整条清账后按我的
		// recLen 重新挂账, 现金与 redoMap 始终同代。
		s.unboundDrain(lsn)
		s.redoMap[lsn] = redoPos{off: off, length: recLen}
		s.unboundReserve(lsn, int64(recLen))
		s.directRedoSegs++ // J5: 直发接受计数(未绑定, 等待 raft meta 建引用)
		s.mu.Unlock()
		return nil
	}
}

// elideMinBytes: don't bother eliding tiny entries (marker ≈ 24B+8B/lsn).
const elideMinBytes = 128

// ElideLogData 是 leader 侧的 P4 指针化: 足够大的 redo 条目把 Data 换成指针 ——
// Data 置空, Extensions 携带 [0xE1][base][lsns][dataLen][crc]; 原条目一个字段不动。
// 不适用时返回 (nil, false), 由 raft 核心走全量复制。
//
// ElideLogData implements raft.LogDataElider (leader side, P4): a redo entry
// big enough to matter goes out as a pointer copy — Data elided, Extensions
// carries [base][lsns][dataLen][crc]. The original entry is untouched.
//
// 算法解读：
//
//	原理：三条前置判据(必须是 ref 模式、必须有 codec、Data 够大且 Extensions 为空)
//	通过后, 用 splitEntry 把 Data 切成 redo 段取出 LSN 列表, 再对整段 Data 算一次
//	crc32c, 一起编码进 elide marker。返回的是**浅拷贝**(cp := *l), 只有 Data 与
//	Extensions 两个字段被改写, 调用方手里的原条目仍是完整的。
//	关键不变量：① 原条目不可被就地改写 —— raft 核心在省略与全量两条路径上都要用
//	它, 回退时靠的就是原条目; ② marker 里的 dataLen/crc 必须针对**整段原始 Data**
//	而非某个段, 因为 follower 侧还原后要拿它与合并结果整体比对; ③ 只有当字节
//	「预期已由 P3 直发面落到 follower 本地」时才值得省略 —— 这个预期不由本函数
//	验证, 而是由 follower 还原失败后回 RedoDataMissingError 来兜底。
//	代价与边界：marker 本身约 24B + 8B/LSN, 故设 elideMinBytes=128 的下限, 小于
//	此值的条目省略后省下的带宽还抵不上 marker 开销。已有 Extensions 的条目直接
//	放弃指针化 —— 不能覆盖业务自己的扩展字段。
//	设计权衡：为什么不在这里主动校验 follower 是否真有这些字节? leader 无从得知
//	(且要额外一次 RPC); 交给 follower 的还原结果反向告知, 并用 DataMissingFrom
//	做粘性回退, 是「乐观省略 + 失败回退」而非「先确认再省略」—— 前者在稳态下零
//	额外往返, 后者每条都要问一次。
func (s *Store) ElideLogData(l *raft.Log) (*raft.Log, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.format != "v2-ref" || s.opts.Codec == nil {
		return nil, false
	}
	// 太小的条目省不下带宽(marker 本身约 24B+8B/LSN); 已有 Extensions 的条目
	// 放弃指针化, 不能覆盖业务自己的扩展字段。
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
	// 浅拷贝: 只改 Data 与 Extensions, 原条目留给回退路径用。
	cp := *l
	cp.Data = nil
	// crc 与长度按**整段原始 Data** 计, 供 follower 合并后整体校验。
	cp.Extensions = raft.EncodeElideMarker(base, lsns, uint32(len(l.Data)), crc32.Checksum(l.Data, castagnoli))
	return &cp, true
}

// ResolveElidedData 是 follower 侧的 P4 还原: 按 elide marker 里的 LSN 从本地
// redo.log 读回各段并合并成 Data; 任一段缺失返回 *raft.RedoDataMissingError, 让
// leader 对该 follower 回退全量复制。两阶段: 抓拍在 mu.RLock, 持链移交
// redoIoMu.RLock 后锁外读盘。
//
// ResolveElidedData implements raft.RedoByteResolver (follower side, P4):
// rebuild the elided entry's Data from the local redo file by LSN. Any
// missing segment → *raft.RedoDataMissingError (leader falls back to full
// data for this follower). 两阶段: 抓拍在 mu.RLock, 持链移交 redoIoMu.RLock
// 后锁外读盘。
//
// 算法解读：
//
//	原理：先 DecodeElideMarker 解出 (base, LSNs, dataLen, crc) —— 解不出(magic
//	不符或长度对不上)直接当「不是省略条目」报错。然后走与 mergeEntry 相同的
//	两阶段读: pinRefReadLocked 抓拍 (fd, 各段位置), 锁外逐段 readRedoAt + 按
//	codec 合并, 最后用 marker 里的 dataLen/crc 校验合并结果。
//	关键不变量：① 失败必须**区分**「LSN 不在 redoMap 里」(真缺失 → 回
//	RedoDataMissingError, leader 会回退全量)与「读出来了但 CRC/长度不符」
//	(数据损坏 → 普通错误, 由 raft 核心按一般失败处理); ② 校验的是合并后的整体
//	Data, 不是逐段 —— 逐段 CRC 只保证段自身完好, 保证不了段序/拼装正确;
//	③ 还原失败时**不能**把带 marker 的条目落盘: 条目要么还原成完整 Data, 要么
//	在写任何东西之前整批失败。
//	代价与边界：段数 N 时 N 次读 + 一次合并; 缺段时先做一遍 O(N) 的 redoMap 查询
//	找出「首个缺失 LSN」以填回执(RedoDataMissingError.LSN), 这个额外查询只在
//	失败路径上发生。多段还原在没有 RedoSegmentCodec 时直接报错, 不做半吊子拼接。
//	设计权衡：为什么由 follower 还原而不是 leader 直接发全量? 稳态下省掉的是整段
//	redo 数据的网络传输(计算层多播已把字节送到各副本), 只有「本地确实没有」时才
//	付全量代价 —— 这就是混合集群(部分副本用 legacy store)仍然安全的原因: 不支持
//	还原的 store 一律走 DataMissingFrom 回退, 无需 leader 预先知道每个 follower 的
//	存储类型。
func (s *Store) ResolveElidedData(ext []byte) ([]byte, error) {
	m := raft.DecodeElideMarker(ext)
	// 解不出 marker(无 0xE1 magic 或长度对不上) = 这不是省略条目, 按普通错误上抛。
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
	// 长度与 CRC 双判: 只比 CRC 会放过极低概率的等长碰撞, 只比长度则完全没有内容校验。
	if uint32(len(data)) != m.DataLen || crc32.Checksum(data, castagnoli) != m.DataCRC {
		return nil, errors.New("filestore: resolved data crc/len mismatch")
	}
	return data, nil
}
