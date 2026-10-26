// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

// 12.14 P4: pointer-tier AppendEntries (redo 条目省略 Data, 指针替代).
//
// With RefReplicationEnabled and a ref-mode log store, the leader MAY elide a
// redo entry's Data when its bytes are (expected to be) already durable on the
// follower via the P3 direct data plane. The pointer rides in Log.Extensions
// (already on the wire):
//
//   Extensions = [0xE1][u32 bodyLen][u64 base][u32 lsnCount][lsns×u64]
//                [u32 dataLen][u32 crc32c(data)]
//
// The follower resolves the marker BEFORE storing: the log store (ref mode)
// reads the payloads from its local redo file by LSN and restores Data in
// memory — the stored entry is complete again, so mixed clusters with stores
// that cannot resolve (legacy/inmem/boltdb) are safe: the follower's raft core
// answers AppendEntriesResponse.DataMissingFrom > 0 and the leader falls back
// to full-data replication for that follower (and sticky-reprobes later).
//
// Correctness notes:
//   - Commit counting is untouched (matchIndex over stored entries); election
//     restriction likewise (term/index live in meta).
//   - Redo semantics make presence idempotent: same LSN ⇒ same bytes.
//   - An elided entry never persists with its marker: resolution either
//     restores Data (store splits + dedup-skips the redo write) or fails the
//     batch before anything is written.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// elideMagicV1 是指针标记的首字节魔数。Log.Extensions 对 raft 内核是不透明字节串
// (见 log.go 的字段说明), 任何中间件都可能往里写东西, 所以用魔数前缀让 follower
// 一眼分辨"这是 P4 指针"还是别人的扩展数据; V1 后缀留给格式演进 —— 改布局就换魔数,
// 认不出的老 follower 会把它当作"未省略"从而退回全量复制, 而不是解析出错误结果。
const elideMagicV1 = 0xE1

// 校验用 Castagnoli(CRC-32C, 多项式 0x1EDC6F41)而非 crc32 的 IEEE 默认表:
// 它在 amd64/arm64 上有专用指令(SSE4.2 的 crc32 / ARMv8 CRC32 扩展), Go 运行时会
// 自动走硬件路径; 同时也是 iSCSI/ext4/btrfs/leveldb 的既定选择, 抗突发错误能力
// 强于 IEEE 多项式。表在这里建一次复用, 避免每条日志重建。
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// ElideMarker 是一条被省略 Data 的 redo 条目所携带的指针(解码后的形态)。
// Base 是 redo 段基址, LSNs 是拼出该条目字节的 LSN 序列(顺序即拼接顺序),
// DataLen/DataCRC 供 follower 还原后做长度 + CRC-32C 双重校验。
//
// ElideMarker is the decoded pointer payload of an elided redo entry.
type ElideMarker struct {
	Base    uint64
	LSNs    []uint64
	DataLen uint32
	DataCRC uint32
}

// EncodeElideMarker 把指针编码成 Log.Extensions 的线格式
// ([0xE1][u32 bodyLen][u64 base][u32 lsnCount][lsns×u64][u32 dataLen][u32 crc32c])。
// 多字节字段一律小端; bodyLen 是 body 自身的字节数, 不含 5 字节头。
//
// EncodeElideMarker serializes the pointer for Log.Extensions.
//
// 算法解读：
//
//	原理：先按"定长 + 变长"拼出 body —— 8 字节 base、4 字节 lsnCount、紧接着
//	      lsnCount 个 u64 的 LSN 序列, 末尾 8 字节放 dataLen 与 dataCRC; 然后
//	      在最前面留出 5 字节写 magic 与 bodyLen, 整体一次 make 出结果。
//	关键不变量：bodyLen 必须等于 len(body), 且 len(body) 必须等于 20 + 8*len(lsns)
//	      —— 解码端正是靠这两条等式把畸形输入挡在切片之外(见 DecodeElideMarker)。
//	      另: 被省略的只是 Data 字节, 索引/任期仍在 Log 的元数据里, 所以提交计数
//	      与选举限制完全不受影响。
//	代价与边界：一次 make + 一次 copy, 时间与空间都线性于 LSN 个数; LSN 极多的
//	      条目(超大 redo 记录)会让指针本身显著变长, 省略的收益被抵消。
//	设计权衡：用显式小端布局而不是 msgpack/反射序列化, 是为了让指针能在 raw-bytes
//	      旁路上零反射地解析, 且字节布局可被未来的 C 侧/内核侧直接读取。
func EncodeElideMarker(base uint64, lsns []uint64, dataLen, dataCRC uint32) []byte {
	body := make([]byte, 8+4+8*len(lsns)+8)
	binary.LittleEndian.PutUint64(body[0:8], base)
	binary.LittleEndian.PutUint32(body[8:12], uint32(len(lsns)))
	for i, lsn := range lsns {
		binary.LittleEndian.PutUint64(body[12+8*i:12+8*i+8], lsn)
	}
	binary.LittleEndian.PutUint32(body[len(body)-8:len(body)-4], dataLen)
	binary.LittleEndian.PutUint32(body[len(body)-4:], dataCRC)
	out := make([]byte, 5+len(body))
	out[0] = elideMagicV1
	binary.LittleEndian.PutUint32(out[1:5], uint32(len(body)))
	copy(out[5:], body)
	return out
}

// DecodeElideMarker 解析 Log.Extensions; 不是省略标记(或畸形)时返回 nil ——
// 调用方一律把 nil 当作"未省略"处理, 这是刻意的设计而非疏漏(见下)。
//
// DecodeElideMarker parses Log.Extensions; returns nil when the extension is
// not an elision marker (or malformed — callers treat that as "not elided").
//
// 算法解读：
//
//	原理：三道纯长度校验之后才按偏移取值 —— ① 总长不小于下界且首字节为魔数;
//	      ② len(ext) == 5 + bodyLen(bodyLen 来自头部自述); ③ len(body) == 20 + 8*n
//	      (n 为 body 里自述的 lsnCount)。三条全过才逐字段读出并复制 LSN 序列。
//	关键不变量：**任何一条不成立都返回 nil 而不是 error**。Extensions 是"别人
//	      也可能在用"的不透明字段, 报错等于让一个带未知扩展的普通日志变成拒绝
//	      服务的输入; 而返回 nil 只是让这条日志按完整 Data 存储, 语义上永远安全。
//	代价与边界：O(n) 复制 LSN 到新切片; 校验只有长度比较, 这里**不算 CRC** ——
//	      指针里没有字节可校, CRC 要等真正还原出 Data 时才算(见 resolveElidedEntries)。
//	设计权衡：先比对自述长度与真实长度、再切片, 避免了"先用 bodyLen 分配再校验";
//	      否则一个 5 字节的畸形输入(自述 bodyLen 极大)就能触发一次巨额分配。
func DecodeElideMarker(ext []byte) *ElideMarker {
	// 5+24: 5 字节头(magic + bodyLen)加 24 字节 body 下界。body 的定长字段是
	// 8(base) + 4(lsnCount) + 8(dataLen, crc32c) = 20 字节, 取 24 是个偏保守的
	// 前置过滤, 目的只是让后面的 ext[0] / ext[1:5] / body[8:12] 切片绝对安全;
	// 真正的合法性由下面两条长度等式精确判定。副作用是 0 个 LSN 的 25 字节指针
	// 会被这条下界挡掉当作"未省略" —— elider 不会产出这种指针, 故无害。
	if len(ext) < 5+24 || ext[0] != elideMagicV1 {
		return nil
	}
	bodyLen := int(binary.LittleEndian.Uint32(ext[1:5]))
	if len(ext) != 5+bodyLen {
		return nil
	}
	body := ext[5:]
	n := int(binary.LittleEndian.Uint32(body[8:12]))
	if len(body) != 8+4+8*n+8 {
		return nil
	}
	m := &ElideMarker{Base: binary.LittleEndian.Uint64(body[0:8]), LSNs: make([]uint64, n)}
	for i := range m.LSNs {
		m.LSNs[i] = binary.LittleEndian.Uint64(body[12+8*i : 12+8*i+8])
	}
	m.DataLen = binary.LittleEndian.Uint32(body[len(body)-8 : len(body)-4])
	m.DataCRC = binary.LittleEndian.Uint32(body[len(body)-4:])
	return m
}

// RedoDataMissingError is returned by a log store that cannot resolve an
// elided entry's bytes from its local redo data.
type RedoDataMissingError struct {
	Index uint64
	LSN   uint64
}

// Error 把缺失的 index/LSN 渲染成一条人可读的错误信息。
func (e *RedoDataMissingError) Error() string {
	return fmt.Sprintf("redo bytes missing for elided entry index=%d lsn=%d", e.Index, e.LSN)
}

// LogDataElider 由 ref 模式的日志存储(leader 侧)实现: 判定一条 redo 条目是否
// 值得省略, 值得则产出它的指针层副本(Data 置空 + Extensions 里放标记)。
//
// LogDataElider is implemented by ref-mode log stores (leader side): produce
// a pointer-tier copy of l (Data nil + Extensions marker) when the entry is a
// redo entry worth eliding.
type LogDataElider interface {
	ElideLogData(l *Log) (elided *Log, ok bool)
}

// RedoByteResolver 由 ref 模式的日志存储(follower 侧)实现: 按 LSN 从本地 redo
// 文件读回被省略条目对应的字节。任一被引用的 LSN 不存在时必须返回
// *RedoDataMissingError, 由调用方转成 AppendEntriesResponse.DataMissingFrom。
//
// RedoByteResolver is implemented by ref-mode log stores (follower side):
// restore the Data bytes of an elided entry from the local redo file.
// Must return *RedoDataMissingError when any referenced LSN is absent.
type RedoByteResolver interface {
	ResolveElidedData(extensions []byte) ([]byte, error)
}

// resolveElidedEntries 就地为带省略标记的条目还原 Data, 返回
// (首个失败条目的 index, 错误); 全部成功时返回 (0, nil)。
//
// resolveElidedEntries restores Data in-place for entries carrying an elision
// marker. Returns (firstMissingIndex, resolverPresent). Stores without a
// resolver never see Data=nil entries slip through: callers must check
// firstMissingIndex and reject those entries back to the leader.
//
// 注意：上面那句 "resolverPresent" 是旧签名的残留, 现签名不返回布尔量, 返回的是
// (第一个无法还原的条目 index, error)。
//
// 算法解读：
//
//	原理：逐条解码 Extensions, 解出指针就要求 store 实现 RedoByteResolver, 由它
//	      按 LSN 从本地 redo 文件读回字节, 再用指针里的 DataLen/DataCRC 做
//	      "长度 + CRC-32C"双校验, 通过后写回 e.Data 并清空 e.Extensions。
//	关键不变量：**elided entry 永不带着 marker 落盘** —— 要么在这里还原成完整
//	      条目(存储层随后按"同 LSN 同前缀"去重并跳过重复的 redo 写), 要么在
//	      写入任何字节之前整批失败。下游存储看到的永远是完整条目, 不存在半条。
//	      另一条: redo 语义使"存在性"幂等(同 LSN ⇒ 同字节), 所以重复解析/重试安全。
//	      长度与 CRC 双校验是必需的 —— 只用长度会放过"同长度不同内容"的错段,
//	      只用 CRC 则无法区分截断与空数据。
//	代价与边界：逐条一次 redo 读盘 + 一次全量 CRC(线性于字节数); 任一条失败即
//	      立即返回, 此前已还原的条目留在内存里、由调用方整批丢弃。空 entries
//	      返回 (0, nil)。
//	设计权衡：把还原放在 follower 的 AppendEntries 路径上, 而不是让每个存储各自
//	      处理, 是为了让"能否还原"变成一个 RPC 响应字段(DataMissingFrom): leader
//	      据此只对该 follower 回退全量复制。混合集群(既有 ref 存储也有 legacy /
//	      内存存储)因此天然安全, 不需要集群级的版本协商或降级开关。
func resolveElidedEntries(store interface{}, entries []*Log) (uint64, error) {
	for _, e := range entries {
		m := DecodeElideMarker(e.Extensions)
		if m == nil {
			continue
		}
		r, ok := store.(RedoByteResolver)
		if !ok {
			return e.Index, fmt.Errorf("store %T cannot resolve elided redo entry", store)
		}
		data, err := r.ResolveElidedData(e.Extensions)
		if err != nil {
			return e.Index, err
		}
		if uint32(len(data)) != m.DataLen || crc32.Checksum(data, castagnoliTable) != m.DataCRC {
			return e.Index, fmt.Errorf("elided entry %d resolved data mismatch (len %d, want %d)", e.Index, len(data), m.DataLen)
		}
		e.Data = data
		// 清空标记, 兑现"elided entry 永不带着 marker 落盘"这条不变量:
		// 下游存储看到的是一条普通完整条目, 它也就不会再走一遍解析。
		e.Extensions = nil // marker resolved; the stored entry is complete
	}
	return 0, nil
}
