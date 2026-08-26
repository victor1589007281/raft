package filestore

import (
	"bytes"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

// TestRefDedupBlockExtension: InnoDB 未满块渐满重写同 start 段([b,b+x)→[b,b+y))
// 是 redo 的合法语义 —— 去重判定必须按"同 LSN 同前缀", 前缀相等且更长时扩展重写,
// 而不是误判 fencing 保留短记录(否则水位在段尾留洞, 提交全部 10s 超时, tps 崩塌)。
func TestRefDedupBlockExtension(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	var logBuf bytes.Buffer
	opts := Options{Mode: ModeRef, Codec: batchTestCodec{}, Logger: log.New(&logBuf, "", 0)}
	s, err := Open(lp, opts)
	if err != nil {
		t.Fatal(err)
	}
	lsn := uint64(1000)
	p1 := bytes.Repeat([]byte("A"), 100)
	// 未满块首写并绑定 [b, b+100)
	if err := s.StoreLog(mkBatchEntry(1, RedoSegment{LSN: lsn, Payload: p1})); err != nil {
		t.Fatal(err)
	}
	// 块渐满重写: 同 start, 前缀相同, 更长 [b, b+200) —— 必须按扩展处理
	p2 := append(bytes.Repeat([]byte("A"), 100), bytes.Repeat([]byte("B"), 100)...)
	if err := s.StoreLog(mkBatchEntry(2, RedoSegment{LSN: lsn, Payload: p2})); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logBuf.String(), "fencing 违例") {
		t.Fatalf("扩展被误判为 fencing: %s", logBuf.String())
	}
	// 条目 2 重建应为完整 200B 信封
	var l raft.Log
	if err := s.GetLog(2, &l); err != nil {
		t.Fatalf("GetLog(2): %v", err)
	}
	want := mkBatchEntry(2, RedoSegment{LSN: lsn, Payload: p2}).Data
	if !bytes.Equal(l.Data, want) {
		t.Fatalf("GetLog(2) 重建不等: got %dB want %dB", len(l.Data), len(want))
	}
	// 条目 1(原短段)仍可读: 前缀字节不变(扩展不改已提交前缀)
	if err := s.GetLog(1, &l); err != nil {
		t.Fatalf("GetLog(1) after extension: %v", err)
	}
	if !bytes.Contains(l.Data, p1) {
		t.Fatal("GetLog(1) 丢失已提交前缀")
	}
	// 更短重发(新段是已存前缀): 保留更长者, 不得截短
	if err := s.StoreLog(mkBatchEntry(3, RedoSegment{LSN: lsn, Payload: p1})); err != nil {
		t.Fatal(err)
	}
	if err := s.GetLog(2, &l); err != nil || !bytes.Equal(l.Data, want) {
		t.Fatalf("更短重发后 GetLog(2) 被截短: %v", err)
	}
	// 未绑定真分叉(direct 路径): 重写修复
	if err := s.WriteRedoDirect(0, 2000, []byte("XXXX")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteRedoDirect(0, 2000, []byte("YYYY")); err != nil {
		t.Fatal(err)
	}
	if p, ok := s.ReadRedo(2000); !ok || string(p) != "YYYY" {
		t.Fatalf("未绑定异字节应重写: %q,%v", p, ok)
	}
	// 已绑定真分叉(direct 路径): fencing 报错且保留已提交字节
	if err := s.StoreLog(mkBatchEntry(4, RedoSegment{LSN: 3000, Payload: []byte("ZZZZ")})); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteRedoDirect(0, 3000, []byte("QQQQ")); err == nil {
		t.Fatal("已绑定分叉应报 fencing 错误")
	}
	if p, ok := s.ReadRedo(3000); !ok || string(p) != "ZZZZ" {
		t.Fatalf("已绑定分叉必须保留已提交字节: %q,%v", p, ok)
	}
	// direct 路径的块渐满扩展: 先短后长, direct 自己也扩展
	if err := s.WriteRedoDirect(0, 4000, []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteRedoDirect(0, 4000, []byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if p, ok := s.ReadRedo(4000); !ok || string(p) != "12345678" {
		t.Fatalf("direct 扩展后应为更长记录: %q,%v", p, ok)
	}
	// 重启后扩展记录持久
	s.Close()
	s2, err := Open(lp, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.GetLog(2, &l); err != nil || !bytes.Equal(l.Data, want) {
		t.Fatalf("重启后 GetLog(2): %v", err)
	}
	if p, ok := s2.ReadRedo(4000); !ok || string(p) != "12345678" {
		t.Fatalf("重启后 direct 扩展记录: %q,%v", p, ok)
	}
}
