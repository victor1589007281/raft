package filestore

// DeleteRange / 重载路径的护栏。
//
// 本文件与 deleterange_invariant_test.go 是一对: 那边把 DeleteRange 的**后置条件**
// (稠密不变量 + 幸存者集合 + 重载后一致)从外部读回来; 这边钉住**保证该不变量成立**的
// 那几道闸门。两边缺一不可 ——
//
//	deleteRangeRewriteLocked 里的 `drop := max - s.first + 1` 是**位置算术**, 它只有在
//	「内存视图稠密 且 s.first == s.logs[0].Index」时才等于「删除的条数」。这个前提不是
//	DeleteRange 自己维护的, 而是由下面三道闸门共同保证的:
//	  ① StoreLogs 的连续性护栏 —— 洞**写不进去**(TestStoreLogsContiguityGuard);
//	  ② 重载时的洞截断           —— 盘上历史遗留的洞在进 NewRaft 前就被截掉(TestReloadGapTruncation);
//	  ③ 整段删除把 first 归零    —— 下一代的 first 不带上一代的索引(deleterange_invariant_test.go)。
//	任何一道失效, drop 就会偏小并把**存活**条目一起切掉, 而且是静默的(返回 nil)。
//
// 这三道闸门曾经只是**设计意图**; #105-B 修好之后快照回退变成可达路径, DeleteRange
// 的调用频率显著上升, 于是它们的正确性从"反正不会走到"变成了"每装一次快照都走一遍"。

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

// TestRefDeleteRangePrefix: ref 模式前缀压实 —— 幸存者 meta 重写, redo 引用
// 依然可解析(GetLog 重建 Data), redo.log 不被截断。
func TestRefDeleteRangePrefix(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, refOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := s.StoreLog(&raft.Log{Index: i, Term: 1, Type: raft.LogCommand,
			Data: []byte{byte('a' + i - 1), byte('0' + i - 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	redoBefore := s.Stats().RedoEndOffset
	if err := s.DeleteRange(1, 6); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().RedoEndOffset; got != redoBefore {
		t.Fatalf("redo end moved on prefix DeleteRange: %d → %d", redoBefore, got)
	}
	for i := uint64(7); i <= 10; i++ {
		var l raft.Log
		if err := s.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) after prefix compaction: %v", i, err)
		}
		if len(l.Data) != 2 || l.Data[0] != byte('a'+i-1) {
			t.Fatalf("GetLog(%d) data = %q", i, l.Data)
		}
	}
	// 重启后仍可重建 + 追加连续
	s.Close()
	s2, err := Open(lp, refOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var l raft.Log
	if err := s2.GetLog(9, &l); err != nil || len(l.Data) != 2 {
		t.Fatalf("post-restart GetLog(9): %v len=%d", err, len(l.Data))
	}
	if first, _ := s2.FirstIndex(); first != 7 {
		t.Fatalf("post-restart first = %d, want 7", first)
	}
	if err := s2.StoreLog(&raft.Log{Index: 11, Term: 2, Type: raft.LogCommand, Data: []byte("k9")}); err != nil {
		t.Fatalf("append after prefix compaction: %v", err)
	}
}

// TestStoreLogsContiguityGuard: 不连续追加必须炸响, 不允许埋洞。
func TestStoreLogsContiguityGuard(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.StoreLogs(mkLogs(1, 5, "g")); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs(mkLogs(7, 1, "g")); err == nil ||
		!strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("gap append should fail, got %v", err)
	}
	// 批内空洞
	bad := []*raft.Log{mkLogs(6, 1, "g")[0], mkLogs(8, 1, "g")[0]}
	if err := s.StoreLogs(bad); err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("intra-batch gap should fail, got %v", err)
	}
	// 合法续接不受影响
	if err := s.StoreLogs(mkLogs(6, 3, "g")); err != nil {
		t.Fatalf("contiguous append: %v", err)
	}
	if last, _ := s.LastIndex(); last != 8 {
		t.Fatalf("last = %d, want 8", last)
	}
}

// TestReloadGapTruncation: 磁盘文件带洞(历史 bug 产物)时, 重载必须从洞处截断,
// 保证 LastIndex/GetLog 一致且后续可续写 —— 而不是带病进 NewRaft 崩循环。
func TestReloadGapTruncation(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs(mkLogs(1, 5, "gap")); err != nil {
		t.Fatal(err)
	}
	// 手工编码 index=8 的记录(跳过 6/7), 直接追加到文件尾部制造空洞
	rec, _, _, err := s.encodeV2(mkLogs(8, 1, "gap")[0])
	if err != nil {
		t.Fatal(err)
	}
	end := s.Stats().EndOffset
	s.Close()

	f, err := os.OpenFile(filepath.Join(lp, "raft.log"), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(rec)))
	if _, err := f.WriteAt(append(lb[:], rec...), end); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if last, _ := s2.LastIndex(); last != 5 {
		t.Fatalf("last after gap truncation = %d, want 5 (洞后条目应被截断)", last)
	}
	if err := s2.GetLog(8, &raft.Log{}); err == nil {
		t.Fatal("GetLog(8) should not survive gap truncation")
	}
	// 洞位续写正常
	if err := s2.StoreLogs(mkLogs(6, 3, "gap2")); err != nil {
		t.Fatalf("rewrite over gap: %v", err)
	}
	var l raft.Log
	if err := s2.GetLog(8, &l); err != nil || l.Index != 8 {
		t.Fatalf("GetLog(8) after rewrite: %v idx=%d", err, l.Index)
	}
	// 再重启: 连续且无残留
	s2.Close()
	s3, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if last, _ := s3.LastIndex(); last != 8 {
		t.Fatalf("final last = %d, want 8", last)
	}
	for i := uint64(1); i <= 8; i++ {
		if err := s3.GetLog(i, &l); err != nil || l.Index != i {
			t.Fatalf("final GetLog(%d): %v idx=%d", i, err, l.Index)
		}
	}
}
