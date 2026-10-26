// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

// 重叠批幂等校验回归(2026-10-24 40k DDL 档误 stepdown)。
//
// 现场: leader 异步持久化报
//   "filestore: non-contiguous append: store last=15007, batch first=15007 (len=1)
//    — refusing to write a hole (overlap content mismatch: incoming term=6 stored term=6)"
// 注意 batch first == store last(重叠, 不是洞) 且两侧 term 相同 —— 不一致只可能来自
// Data 字节比较, 而 Data 的字节形态在同一 index/term 下本来就可以合法地不同:
//   - ref 指针形态: 字节在 redo.log, 内存视图 Data=nil;
//   - 省略标记形态: Extensions 带 elide marker, Data 被摘掉;
//   - codec 归一的形态: 编解码往返只承诺语义等价(单段批归一成单条信封),
//     v2 文件重载时 decodeV2 走 Codec.Merge 重建 Data。
// 拿这些形态做 bytes.Equal 必然误报 ⇒ asyncWriteFailed ⇒ leader step down。
//
// 本文件覆盖三种边界的完整矩阵(见 StoreLogs 函数头注释):
//   ① 重叠且一致       ⇒ 幂等成功(no-op / 只追加新尾), last 不重复
//   ② 重叠但不一致     ⇒ fail-loud
//   ③ batch first > last+1(真空洞) ⇒ 拒绝

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// logdbLikeCodec 复刻 LogDB 的 logdbRedoCodec 的关键性质:
// 编解码往返【语义等价但字节不保证相等】——
// 单段批 [0x02][base][count=1][lsn][len][payload] 经 MergeSegments 会归一成
// 单条信封 [0x01][base][lsn][payload], 与原字节不同。
type logdbLikeCodec struct{}

const (
	testSingleMagic byte = 0x01
	testBatchMagic  byte = 0x02
)

func (logdbLikeCodec) SplitSegments(data []byte) (uint64, []RedoSegment) {
	if len(data) < 13 {
		return 0, nil
	}
	base := binary.BigEndian.Uint64(data[1:9])
	switch data[0] {
	case testSingleMagic:
		lsn := binary.BigEndian.Uint64(data[9:17])
		return base, []RedoSegment{{LSN: lsn, Payload: data[17:]}}
	case testBatchMagic:
		n := int(binary.BigEndian.Uint32(data[9:13]))
		pos := 13
		segs := make([]RedoSegment, 0, n)
		for i := 0; i < n; i++ {
			if pos+12 > len(data) {
				return 0, nil
			}
			lsn := binary.BigEndian.Uint64(data[pos : pos+8])
			ln := int(binary.BigEndian.Uint32(data[pos+8 : pos+12]))
			pos += 12
			if pos+ln > len(data) {
				return 0, nil
			}
			segs = append(segs, RedoSegment{LSN: lsn, Payload: data[pos : pos+ln]})
			pos += ln
		}
		return base, segs
	default:
		return 0, nil // 非 redo 命令行: 0 段(内联)
	}
}

func (logdbLikeCodec) MergeSegments(base uint64, segs []RedoSegment) []byte {
	if len(segs) == 1 {
		// ★归一化: 单段一律产出单条信封(与原批信封字节不同, 语义相同)
		out := make([]byte, 17, 17+len(segs[0].Payload))
		out[0] = testSingleMagic
		binary.BigEndian.PutUint64(out[1:9], base)
		binary.BigEndian.PutUint64(out[9:17], segs[0].LSN)
		return append(out, segs[0].Payload...)
	}
	out := make([]byte, 13, 13+len(segs)*16)
	out[0] = testBatchMagic
	binary.BigEndian.PutUint64(out[1:9], base)
	binary.BigEndian.PutUint32(out[9:13], uint32(len(segs)))
	var h [12]byte
	for _, s := range segs {
		binary.BigEndian.PutUint64(h[0:8], s.LSN)
		binary.BigEndian.PutUint32(h[8:12], uint32(len(s.Payload)))
		out = append(out, h[:]...)
		out = append(out, s.Payload...)
	}
	return out
}

func (c logdbLikeCodec) Split(l *raft.Log) (uint64, uint64, []byte) {
	base, segs := c.SplitSegments(l.Data)
	if len(segs) == 1 {
		return base, segs[0].LSN, segs[0].Payload
	}
	return 0, 0, l.Data
}

func (c logdbLikeCodec) Merge(base, lsn uint64, payload []byte) []byte {
	return c.MergeSegments(base, []RedoSegment{{LSN: lsn, Payload: payload}})
}

// mkRedoBatch 造一条 cmdBatch 风格的 redo 命令(lsns 与 payloads 一一对应)。
func mkRedoBatch(idx, term, base uint64, lsns []uint64, payloads ...string) *raft.Log {
	segs := make([]RedoSegment, len(payloads))
	for i := range payloads {
		segs[i] = RedoSegment{LSN: lsns[i], Payload: []byte(payloads[i])}
	}
	return &raft.Log{Index: idx, Term: term, Type: raft.LogCommand,
		Data: logdbLikeCodec{}.MergeSegments(base, segs)}
}

// mkRedoSingle 造一条 cmdSingle 风格的 redo 命令。
func mkRedoSingle(idx, term, base, lsn uint64, payload string) *raft.Log {
	return &raft.Log{Index: idx, Term: term, Type: raft.LogCommand,
		Data: logdbLikeCodec{}.MergeSegments(base, []RedoSegment{{LSN: lsn, Payload: []byte(payload)}})}
}

// mkRawBatch 强制按"批信封"编码(哪怕只有一段) —— LogDB 的 encodeBatchBase 就是
// 这么写的(ApplyRedoBatchBase 单条也发 cmdBatch), 用来复刻重载后被 codec 归一化
// 的那个现场。
func mkRawBatch(idx, term, base uint64, lsns []uint64, payloads ...string) *raft.Log {
	out := make([]byte, 13, 13+16*len(payloads))
	out[0] = testBatchMagic
	binary.BigEndian.PutUint64(out[1:9], base)
	binary.BigEndian.PutUint32(out[9:13], uint32(len(payloads)))
	var h [12]byte
	for i, p := range payloads {
		binary.BigEndian.PutUint64(h[0:8], lsns[i])
		binary.BigEndian.PutUint32(h[8:12], uint32(len(p)))
		out = append(out, h[:]...)
		out = append(out, p...)
	}
	return &raft.Log{Index: idx, Term: term, Type: raft.LogCommand, Data: out}
}

func redoLogs(start, n uint64) []*raft.Log {
	logs := make([]*raft.Log, n)
	for i := range logs {
		idx := start + uint64(i)
		lsn := 1000 + idx
		logs[i] = mkRedoBatch(idx, 6, lsn-5, []uint64{lsn, lsn + 1},
			fmt.Sprintf("p%d-a", idx), fmt.Sprintf("p%d-b", idx))
	}
	return logs
}

func storeOverlap(t *testing.T, s *Store, logs []*raft.Log) {
	t.Helper()
	if err := s.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs([%d..%d]): %v", logs[0].Index, logs[len(logs)-1].Index, err)
	}
}

func mustLast(t *testing.T, s *Store, want uint64) {
	t.Helper()
	last, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != want {
		t.Fatalf("LastIndex = %d, want %d", last, want)
	}
}

// sameCanonical 断言两条目在"归一形态"下内容一致(ref 读取会重建 Data)。
func sameCanonical(t *testing.T, s *Store, got, want *raft.Log) {
	t.Helper()
	if !bytes.Equal(s.canonicalEntryData(got.Data), s.canonicalEntryData(want.Data)) {
		t.Fatalf("entry %d content mismatch after idempotent rewrite:\n got=%x\nwant=%x",
			got.Index, got.Data, want.Data)
	}
}

// ---- ① 重叠且一致 ⇒ 幂等成功 ----

// TestOverlapIdenticalTailIsNoop: 追加 [1..N] 后再提交一个从 N 开始、内容一致的
// 批 ⇒ 成功, last 不变, 不产生重复条目(ref 指针形态)。
func TestOverlapIdenticalTailIsNoop(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{Mode: ModeRef, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 8
	logs := redoLogs(1, n)
	storeOverlap(t, s, logs)
	mustLast(t, s, n)
	endOff := s.Stats().EndOffset

	// 整批重投: batch first == store last ⇒ 重叠 1 条, term/内容一致
	if err := s.StoreLogs([]*raft.Log{redoLogs(n, 1)[0]}); err != nil {
		t.Fatalf("idempotent tail overlap must succeed, got %v", err)
	}
	// 多批重投(异步写者合并/重派的形状)
	storeOverlap(t, s, redoLogs(n-2, 3))
	mustLast(t, s, n)
	if got := s.Stats().EndOffset; got != endOff {
		t.Fatalf("no-op overlap must not append bytes: endOff %d → %d", endOff, got)
	}

	// 内容没被改写(重复条目/错位都会在这里暴露)
	var got raft.Log
	if err := s.GetLog(n, &got); err != nil {
		t.Fatalf("GetLog(%d): %v", n, err)
	}
	sameCanonical(t, s, &got, redoLogs(n, 1)[0])
}

// TestOverlapPrefixBeforeTailAppendsOnlyNew: 从 N-2 开始、前缀一致而后缀新增的批
// ⇒ 只追加真正新增的条目。
func TestOverlapPrefixBeforeTailAppendsOnlyNew(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{SingleWAL: true, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 10
	storeOverlap(t, s, redoLogs(1, n))
	mustLast(t, s, n)
	endOff := s.Stats().EndOffset

	// 8..14: 8/9/10 与盘上一致, 11..14 是新的
	batch := redoLogs(8, 7)
	storeOverlap(t, s, batch)
	mustLast(t, s, 14)
	if got := s.Stats().EndOffset; got <= endOff {
		t.Fatalf("new tail must be appended: endOff %d → %d", endOff, got)
	}
	for _, want := range batch[3:] { // 11..14
		var got raft.Log
		if err := s.GetLog(want.Index, &got); err != nil {
			t.Fatalf("GetLog(%d): %v", want.Index, err)
		}
		sameCanonical(t, s, &got, want)
	}
	// 重叠前缀没被重复写: 8..10 仍然是原内容
	for _, want := range redoLogs(1, n)[7:] {
		var got raft.Log
		if err := s.GetLog(want.Index, &got); err != nil {
			t.Fatalf("GetLog(%d): %v", want.Index, err)
		}
		sameCanonical(t, s, &got, want)
	}
}

// TestOverlapRefPointerVsFullData: ref 模式下已落盘条目内存视图 Data=nil
// (指针在 raft.log, 字节在 redo.log), 重投批携带完整 Data ⇒ 幂等成功。
// 这正是"一侧 ref 形态、一侧完整数据"的现场形状。
func TestOverlapRefPointerVsFullData(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{Mode: ModeRef, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 5
	logs := redoLogs(1, n-1)
	// 末条用"单段但按批信封写"的形态, 便于构造同一内容的另一种编码
	logs = append(logs, mkRawBatch(n, 6, 777, []uint64{888}, fmt.Sprintf("p%d-a", n)))
	storeOverlap(t, s, logs)
	mustLast(t, s, n)

	// (a) 一侧 ref 指针(内存 Data=nil)、一侧完整数据: 原字节重投
	if err := s.StoreLogs([]*raft.Log{logs[n-1]}); err != nil {
		t.Fatalf("ref-pointer overlap (full data) must be idempotent, got %v", err)
	}
	// (b) 同一逻辑命令的另一种 ref 编码: 单条信封(段清单相同 ⇒ 语义指纹相同)
	single := mkRedoSingle(n, 6, 777, 888, fmt.Sprintf("p%d-a", n))
	if err := s.StoreLogs([]*raft.Log{single}); err != nil {
		t.Fatalf("ref-pointer overlap (alternate ref encode) must be idempotent, got %v", err)
	}
	mustLast(t, s, n)
}

// TestOverlapNormalizedEncodeAfterReload: v2 单文件重载后, decodeV2 用 Codec.Merge
// 重建 Data —— 单段批会被归一成单条信封(字节与原批不同)。此时重投原字节批必须
// 幂等成功, 而不是被判成"内容冲突"⇒stepdown。这就是 2026-10-24 现场的直接复刻。
func TestOverlapNormalizedEncodeAfterReload(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	opts := Options{SingleWAL: true, Codec: logdbLikeCodec{}}
	s, err := Open(lp, opts)
	if err != nil {
		t.Fatal(err)
	}
	// 第 3 条是【单段批】: 盘上写的是 [0x02][base][count=1][lsn][len][payload] 形态
	logs := []*raft.Log{
		mkRedoSingle(1, 6, 100, 101, "a"),
		mkRedoSingle(2, 6, 110, 111, "b"),
		mkRawBatch(3, 6, 120, []uint64{121}, "c"),
	}
	storeOverlap(t, s, logs)
	if logs[2].Data[0] != testBatchMagic {
		t.Fatalf("precondition: entry 3 should be encoded as a batch envelope")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(lp, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	mustLast(t, s2, 3)

	// 前提: 重载后第 3 条的 Data 已被 codec 归一成单条信封(字节与原批不同)
	var reloaded raft.Log
	if err := s2.GetLog(3, &reloaded); err != nil {
		t.Fatalf("GetLog(3) after reload: %v", err)
	}
	if reloaded.Data[0] != testSingleMagic {
		t.Fatalf("precondition: reload should normalize entry 3 to the single envelope, got magic %#x", reloaded.Data[0])
	}
	if string(reloaded.Data) == string(logs[2].Data) {
		t.Fatalf("precondition: normalized bytes must differ from the original batch bytes")
	}

	// 重投原字节(现场: 异步写者重派/重试批) ⇒ 必须幂等成功
	if err := s2.StoreLogs([]*raft.Log{logs[2]}); err != nil {
		t.Fatalf("normalized-form overlap must be idempotent, got %v", err)
	}
	mustLast(t, s2, 3)
}

// TestOverlapBelowCompactedFirstIsNoop: 批首落在已被压实回收的前缀里
// (字节已不在盘上)⇒ 前缀按幂等跳过, 只追加越过 last 的新尾, 不报错。
func TestOverlapBelowCompactedFirstIsNoop(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{SingleWAL: true, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	storeOverlap(t, s, redoLogs(1, 10))
	if err := s.DeleteRange(1, 5); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if first, _ := s.FirstIndex(); first != 6 {
		t.Fatalf("first = %d, want 6", first)
	}
	// 4..12: 4/5 已压实, 6..10 与盘上一致, 11/12 是新的
	batch := redoLogs(4, 9)
	if err := s.StoreLogs(batch); err != nil {
		t.Fatalf("overlap into compacted prefix must be idempotent, got %v", err)
	}
	mustLast(t, s, 12)
	var got raft.Log
	if err := s.GetLog(12, &got); err != nil {
		t.Fatalf("GetLog(12): %v", err)
	}
	sameCanonical(t, s, &got, batch[8])
}

// ---- ② 重叠但不一致 ⇒ fail-loud ----

// TestOverlapContentConflictFails: 同 index/term、两侧都是 inline 且真实内容不同
// ⇒ 真冲突, 必须拒绝。
func TestOverlapContentConflictFails(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{SingleWAL: true, SparseIndex: true}) // 无 codec: 纯 inline 字节
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.StoreLogs([]*raft.Log{
		{Index: 1, Term: 6, Type: raft.LogCommand, Data: []byte("one")},
		{Index: 2, Term: 6, Type: raft.LogCommand, Data: []byte("two")},
	}); err != nil {
		t.Fatal(err)
	}
	// 同 index 同 term, 内容不同
	err = s.StoreLogs([]*raft.Log{{Index: 2, Term: 6, Type: raft.LogCommand, Data: []byte("TWO-different")}})
	if err == nil || !strings.Contains(err.Error(), "non-contiguous") ||
		!strings.Contains(err.Error(), "content conflict") {
		t.Fatalf("content conflict must fail-loud with a diagnostic, got %v", err)
	}
	mustLast(t, s, 2)
	// 原内容不得被覆盖
	var got raft.Log
	if err := s.GetLog(2, &got); err != nil || string(got.Data) != "two" {
		t.Fatalf("stored entry must be untouched: %v data=%q", err, got.Data)
	}
}

// TestOverlapRedoContentConflictFails: redo 条目(有 codec)语义内容不同 —— 即使
// 归一化之后仍然不同 ⇒ fail-loud。保证"取消字节比较"没有把真冲突也放过去。
func TestOverlapRedoContentConflictFails(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{Mode: ModeRef, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	storeOverlap(t, s, redoLogs(1, 3))
	// 同 index(3)/term(6), 但 payload 与 LSN 都不同 ⇒ 归一后依旧不同
	conflict := mkRedoBatch(3, 6, 7777, []uint64{8888}, "completely-different")
	err = s.StoreLogs([]*raft.Log{conflict})
	if err == nil || !strings.Contains(err.Error(), "non-contiguous") ||
		!strings.Contains(err.Error(), "redo segment conflict") {
		t.Fatalf("redo content conflict must fail-loud, got %v", err)
	}
	mustLast(t, s, 3)
}

// TestOverlapTermConflictFails: 同 index 不同 term ⇒ 真冲突(日志分叉)。
func TestOverlapTermConflictFails(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{Mode: ModeRef, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	storeOverlap(t, s, redoLogs(1, 3))
	conflict := redoLogs(3, 1)[0]
	conflict.Term = 7
	err = s.StoreLogs([]*raft.Log{conflict})
	if err == nil || !strings.Contains(err.Error(), "index/term conflict") {
		t.Fatalf("term conflict must fail-loud, got %v", err)
	}
	mustLast(t, s, 3)
}

// ---- ③ 真空洞 ⇒ 保持拒绝 ----

func TestOverlapGapStillRefused(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{Mode: ModeRef, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	storeOverlap(t, s, redoLogs(1, 5))
	// batch first = 7 > last+1 = 6 ⇒ 真空洞
	err = s.StoreLogs(redoLogs(7, 1))
	if err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("gap must be refused, got %v", err)
	}
	// 批内空洞
	bad := []*raft.Log{redoLogs(6, 1)[0], redoLogs(8, 1)[0]}
	if err := s.StoreLogs(bad); err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("intra-batch gap must be refused, got %v", err)
	}
	mustLast(t, s, 5)
	// 合法续接不受影响
	storeOverlap(t, s, redoLogs(6, 3))
	mustLast(t, s, 8)
}

// ---- 异步持久化: 幂等重叠不得触发 stepdown ----

// normalizingResendStore 模拟"同一批以另一编码再落一次盘"的现场(异步写者重派/
// 重试 + codec 归一): 先按原字节写, 再把同一批按归一形态重写一次 —— 第二次调用
// 的 batch first <= store last, 且 Data 字节不同但语义相同。
// 旧护栏在这里误判成内容冲突 ⇒ asyncWriteFailed ⇒ leader step down。
type normalizingResendStore struct {
	inner *Store
	codec RedoSegmentCodec
}

func (w *normalizingResendStore) FirstIndex() (uint64, error) { return w.inner.FirstIndex() }
func (w *normalizingResendStore) LastIndex() (uint64, error)  { return w.inner.LastIndex() }
func (w *normalizingResendStore) GetLog(i uint64, l *raft.Log) error {
	return w.inner.GetLog(i, l)
}
func (w *normalizingResendStore) DeleteRange(a, b uint64) error { return w.inner.DeleteRange(a, b) }
func (w *normalizingResendStore) StoreLog(l *raft.Log) error    { return w.StoreLogs([]*raft.Log{l}) }

func (w *normalizingResendStore) StoreLogs(logs []*raft.Log) error {
	if err := w.inner.StoreLogs(logs); err != nil {
		return err
	}
	resend := make([]*raft.Log, 0, len(logs))
	for _, l := range logs {
		base, segs := w.codec.SplitSegments(l.Data)
		if len(segs) == 0 {
			continue // 非 redo 命令行(noop/config): 编码本就是原字节, 不制造形态差异
		}
		cp := *l
		cp.Data = w.codec.MergeSegments(base, segs)
		resend = append(resend, &cp)
	}
	if len(resend) == 0 {
		return nil
	}
	return w.inner.StoreLogs(resend)
}

// TestAsyncLeaderPersistIdempotentOverlapStaysLeader: 异步持久化路径上出现幂等
// 重叠(同 index/term, 字节形态不同)时, StoreLogs 必须返回成功; leader 不得
// step down, 提交必须继续推进。
func TestAsyncLeaderPersistIdempotentOverlapStaysLeader(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	store, err := Open(lp, Options{SingleWAL: true, Codec: logdbLikeCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stable, err := OpenStable(lp)
	if err != nil {
		t.Fatal(err)
	}
	snaps := raft.NewInmemSnapshotStore()
	addr, trans := raft.NewInmemTransport("")
	logs := &normalizingResendStore{inner: store, codec: logdbLikeCodec{}}

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID("n1")
	conf.AsyncLeaderPersist = true
	conf.HeartbeatTimeout = 200 * time.Millisecond
	conf.ElectionTimeout = 200 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.CommitTimeout = 10 * time.Millisecond
	conf.Logger = nil
	conf.LogOutput = discardWriter{}
	initial := raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: conf.LocalID, Address: addr}}}
	if err := raft.BootstrapCluster(conf, logs, stable, snaps, trans, initial); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	r, err := raft.NewRaft(conf, &raft.MockFSM{}, logs, stable, snaps, trans)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	defer func() { _ = r.Shutdown().Error() }()

	deadline := time.Now().Add(10 * time.Second)
	for r.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("node did not become leader, state=%v", r.State())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 每个 Apply 都会经历"原字节写 + 归一形态重写"(内部制造重叠)。
	// 命令用【批信封】形态提交: 归一后被改写成单条信封, 字节不同而语义相同 ——
	// 正是现场"同一 (index,term) 两种编码"的形状。
	for i := 0; i < 30; i++ {
		cmd := mkRawBatch(0, 0, uint64(1000+i), []uint64{uint64(2000 + i)}, fmt.Sprintf("cmd-%d", i)).Data
		f := r.Apply(cmd, 5*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("apply #%d failed (async persist saw an error → stepdown?): %v", i, err)
		}
	}
	if got := r.State(); got != raft.Leader {
		t.Fatalf("leader stepped down after idempotent overlaps: state=%v", got)
	}
	if got, want := r.AppliedIndex(), uint64(30); got < want {
		t.Fatalf("applied index = %d, want >= %d", got, want)
	}
	if last, _ := store.LastIndex(); last == 0 {
		t.Fatalf("store last index = 0, nothing persisted")
	}
}

// discardWriter 吞掉 raft 的日志输出(测试噪声控制)。
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
