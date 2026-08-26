package filestore

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

// TestDeleteRangePrefixKeepsSuffix: raft 快照压实是前缀删除 [first, K] ——
// (K, last] 必须存活(历史上这里误用截断,把整个日志抹掉)。v2-singlewal。
func TestDeleteRangePrefixKeepsSuffix(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs(mkLogs(1, 10, "pfx")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRange(1, 6); err != nil {
		t.Fatal(err)
	}
	if first, _ := s.FirstIndex(); first != 7 {
		t.Fatalf("first = %d, want 7", first)
	}
	if last, _ := s.LastIndex(); last != 10 {
		t.Fatalf("last = %d, want 10", last)
	}
	for i := uint64(7); i <= 10; i++ {
		var l raft.Log
		if err := s.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) after prefix compaction: %v", i, err)
		}
		if l.Index != i {
			t.Fatalf("GetLog(%d) returned entry %d", i, l.Index)
		}
	}
	if err := s.GetLog(6, &raft.Log{}); err == nil {
		t.Fatal("compacted GetLog(6) should fail")
	}
	if _, ok := s.LookupByLSN(5); ok {
		t.Fatal("sparse index not pruned for compacted prefix")
	}
	if _, ok := s.LookupByLSN(8); !ok {
		t.Fatal("survivor lsn pruned")
	}
	// 重启后存活 + 可继续连续追加
	s.Close()
	s2, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if first, _ := s2.FirstIndex(); first != 7 {
		t.Fatalf("post-restart first = %d, want 7", first)
	}
	var l raft.Log
	if err := s2.GetLog(10, &l); err != nil || l.Index != 10 {
		t.Fatalf("post-restart GetLog(10): %v idx=%d", err, l.Index)
	}
	if err := s2.StoreLogs(mkLogs(11, 2, "pfx2")); err != nil {
		t.Fatalf("append after prefix compaction: %v", err)
	}
	if err := s2.GetLog(12, &l); err != nil {
		t.Fatalf("GetLog(12): %v", err)
	}
}

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
