package filestore

// #105-C: DeleteRange 的两条路径必须满足的后置条件。
//
// 为什么现在才钉: #105-B 修好之前, replication.go 里 `err == ErrLogNotFound` 是**身份比较**,
// 而 filestore 的空洞分支返回 `%w` **包装**过的哨兵 ⇒ SEND_SNAP 永不可达 ⇒ **InstallSnapshot
// 在生产里几乎从不发生**, 于是 DeleteRange / RecoverCluster 这条路径长期缺乏真实流量,
// 它的后置条件也就没人从外部读回。修好 #105-B 之后快照回退变成可达路径, 这条删除路径的
// 调用频率会**显著上升** —— 现在是钉住它的时机, 而不是等到它在生产里删错东西再回头查。
//
// 怀疑点是 `deleteRangeRewriteLocked` 里的位置算术:
//
//	drop := int(max - s.first + 1) // 删除 [first, max], 保留 s.logs[drop:]
//
// 它**假设 s.logs 稠密且 s.logs[0].Index == s.first** —— 若该不变量不成立, drop 会偏小,
// 于是把本该**存活**的 (max, last] 条目一起切掉, 且是静默的(返回 nil)。
// 本文件不去断言"代码有没有这个 bug", 而是把不变量本身**测出来**: 先证明它成立,
// 再让每条路径都从外部把后置条件读回来。

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

// denseInvariant 把「内存视图稠密 + first 对齐 logs[0]」一次性断言掉,
// 失败时给出够定位的现场(而不是只说"不相等")。
func denseInvariant(t *testing.T, s *Store, where string) {
	t.Helper()
	if len(s.logs) == 0 {
		if s.first != 0 {
			t.Fatalf("%s: logs 为空但 first=%d (重载约定: 空 ⇒ first=0)", where, s.first)
		}
		return
	}
	if got := s.logs[0].Index; got != s.first {
		t.Fatalf("%s: first=%d 但 logs[0].Index=%d —— drop=max-first+1 的位置算术会算错",
			where, s.first, got)
	}
	for i := 1; i < len(s.logs); i++ {
		if want := s.logs[i-1].Index + 1; s.logs[i].Index != want {
			t.Fatalf("%s: 内存视图不稠密: logs[%d].Index=%d, 期望 %d",
				where, i, s.logs[i].Index, want)
		}
	}
}

// TestDeleteRangePrefixRewriteKeepsExactlyTheSurvivors 前缀/中段删除(min <= first, max < last)
// 必须**恰好**保留 (max, last], 既不多删存活条目, 也不漏删该删的。
func TestDeleteRangePrefixRewriteKeepsExactlyTheSurvivors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logs")
	s, err := Open(path, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// [10, 29], first=10 last=29, 共 20 条。
	if err := s.StoreLogs(mkLogs(10, 20, "dr")); err != nil {
		t.Fatal(err)
	}
	denseInvariant(t, s, "写入后")

	// 删除 [10, 16] (min <= first, max < last) ⇒ 幸存者应为 [17, 29]。
	if err := s.DeleteRange(10, 16); err != nil {
		t.Fatalf("DeleteRange(10,16): %v", err)
	}
	denseInvariant(t, s, "前缀重写后")

	var want []uint64
	for i := uint64(17); i <= 29; i++ {
		want = append(want, i)
	}
	got := make([]uint64, 0, len(s.logs))
	for _, l := range s.logs {
		got = append(got, l.Index)
	}
	if len(got) != len(want) {
		t.Fatalf("幸存者条数 = %d, 期望 %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("幸存者第 %d 项 = %d, 期望 %d\n got=%v\nwant=%v", i, got[i], want[i], got, want)
		}
	}
	if s.first != 17 {
		t.Fatalf("first = %d, 期望 17", s.first)
	}
	// 被删的必须真的读不回来, 且**存活的一条都不能少** —— 这是本条用例的主判据。
	if err := s.GetLog(16, &raft.Log{}); err == nil {
		t.Fatal("GetLog(16) 应当失败(已删除)")
	}
	// 稀疏索引必须同步剪掉被删段 —— 否则 LookupByLSN 会把一个已删除的 LSN
	// 解析成一个仍在的位置, 上层据此读到别的记录。
	if _, ok := s.LookupByLSN(12); ok {
		t.Fatal("稀疏索引未剪掉已删除段的 LSN")
	}
	if _, ok := s.LookupByLSN(20); !ok {
		t.Fatal("稀疏索引误剪了存活段的 LSN")
	}
	for i := uint64(17); i <= 29; i++ {
		var l raft.Log
		if err := s.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) 应当存在(存活), 却失败: %v", i, err)
		}
		if l.Index != i {
			t.Fatalf("GetLog(%d) 读回索引 %d", i, l.Index)
		}
	}

	// ★重载后仍须成立: 内存视图与磁盘必须一致(否则重载会看到不同答案)。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	denseInvariant(t, s2, "重载后")
	if s2.first != 17 {
		t.Fatalf("重载后 first = %d, 期望 17", s2.first)
	}
	if got := s2.lastIndexLocked(); got != 29 {
		t.Fatalf("重载后 lastIndex = %d, 期望 29", got)
	}
}

// TestDeleteRangeTruncationPathKeepsFirstAligned 廉价截断路径(min > first)。
// 它**不重算 first**, 只切 slices —— 本用例把"因此 first 必须仍等于 logs[0].Index"读回来。
func TestDeleteRangeTruncationPathKeepsFirstAligned(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logs")
	s, err := Open(path, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.StoreLogs(mkLogs(100, 20, "tr")); err != nil {
		t.Fatal(err)
	}
	denseInvariant(t, s, "写入后")

	// min=105 > first=100, max=last(124) ⇒ 走截断路径。幸存者 [100, 104]。
	if err := s.DeleteRange(105, 124); err != nil {
		t.Fatalf("DeleteRange(105,124): %v", err)
	}
	denseInvariant(t, s, "截断路径后")
	if s.first != 100 {
		t.Fatalf("first = %d, 期望 100 (截断路径不应改 first)", s.first)
	}
	if got := s.lastIndexLocked(); got != 104 {
		t.Fatalf("lastIndex = %d, 期望 104", got)
	}
	for i := uint64(100); i <= 104; i++ {
		var l raft.Log
		if err := s.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) 应当存活: %v", i, err)
		}
	}
	if err := s.GetLog(105, &raft.Log{}); err == nil {
		t.Fatal("GetLog(105) 应当已删除")
	}
}

// TestDeleteRangeEntirelyBeyondLastIsANoOp 删除区间整个落在日志之后时必须**什么都不做**,
// 而不是越界 panic。
//
// 修前: keep = min-first 被 clamp 到 len(logs), 但紧接着 `truncAt := s.offsets[keep]`
// 仍用夹住后的值索引 ⇒ index out of range [20] with length 20。那个 clamp 只护住了
// 切片上界, 没护住真正被索引的偏移 —— panic 会顺着 LogStore 接口把整个存储进程带走。
//
// 语义上与已有的 `max < s.first`(区间整个在日志之前)对称: 无事可做 ⇒ nil。
func TestDeleteRangeEntirelyBeyondLastIsANoOp(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.StoreLogs(mkLogs(10, 20, "oob")); err != nil { // first=10 last=29
		t.Fatal(err)
	}
	before := s.lastIndexLocked()

	// 边界两侧: min == last+1(紧邻之外) 与 min 远在之外, 都不得改变任何东西。
	for _, tc := range []struct{ min, max uint64 }{
		{30, 35}, // min == last+1
		{31, 31}, // min > last, min == max
		{40, 45}, // 远在之外(修前在此 panic)
	} {
		if err := s.DeleteRange(tc.min, tc.max); err != nil {
			t.Fatalf("DeleteRange(%d,%d) 应当是无事可做的 no-op, 却报错: %v", tc.min, tc.max, err)
		}
		if got := s.lastIndexLocked(); got != before {
			t.Fatalf("DeleteRange(%d,%d) 之后 lastIndex = %d, 期望不变(%d)", tc.min, tc.max, got, before)
		}
		if s.first != 10 {
			t.Fatalf("DeleteRange(%d,%d) 之后 first = %d, 期望不变(10)", tc.min, tc.max, s.first)
		}
	}

	// 全部存活条目必须原样还在 —— no-op 的意思是"一条都没动"。
	for i := uint64(10); i <= 29; i++ {
		var l raft.Log
		if err := s.GetLog(i, &l); err != nil || l.Index != i {
			t.Fatalf("no-op 之后 GetLog(%d) = %v idx=%d, 全部条目本应原样保留", i, err, l.Index)
		}
	}
	denseInvariant(t, s, "越界 no-op 后")
}

// TestDeleteRangeFullWipeResetsFirst 整段删除(max >= last)必须把 first 归零、logs 清空 ——
// 否则下一次 append 的 first 起点会带着上一代的索引, 稠密不变量当场断掉。
func TestDeleteRangeFullWipeResetsFirst(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.StoreLogs(mkLogs(50, 10, "fw")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRange(50, 59); err != nil {
		t.Fatalf("DeleteRange(50,59): %v", err)
	}
	denseInvariant(t, s, "整段删除后")
	if s.first != 0 || len(s.logs) != 0 {
		t.Fatalf("整段删除后 first=%d len(logs)=%d, 期望 0/0", s.first, len(s.logs))
	}

	// 从新的起点重新追加 —— 这才是"归零"真正保护的动作。
	if err := s.StoreLogs(mkLogs(1000, 5, "fw2")); err != nil {
		t.Fatalf("整段删除后重新追加: %v", err)
	}
	denseInvariant(t, s, "重新追加后")
	if s.first != 1000 {
		t.Fatalf("重新追加后 first = %d, 期望 1000", s.first)
	}
}
