// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// ---------------------------------------------------------------------------
// P3: WriteRedoDirect — 直发数据面(未绑定直写 + 幂等去重 + 背压)

func TestWriteRedoDirectDedup(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), refOpts())
	if err != nil {
		t.Fatal(err)
	}
	// P3: 计算层直发先到(redo.log 承载数据写)
	if err := s.WriteRedoDirect(500, 500, []byte("direct-payload")); err != nil {
		t.Fatal(err)
	}
	redoAfter := s.Stats().RedoEndOffset
	if _, ok := s.redoMap[500]; !ok {
		t.Fatal("direct write not in presence map")
	}
	// raft 绑定后到: StoreLogs 必须跳过重写(单副本), redoEnd 不动
	l := &raft.Log{Index: 500, Term: 1, Type: raft.LogCommand, Data: []byte("direct-payload")} // IndexLSNCodec: lsn=index=500 才能命中去重
	if err := s.StoreLog(l); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().RedoEndOffset; got != redoAfter {
		t.Fatalf("dedup failed: redoEnd moved %d → %d on bind", redoAfter, got)
	}
	if s.unboundBytes != 0 {
		t.Fatalf("unbound not drained after bind: %d", s.unboundBytes)
	}
	var got raft.Log
	if err := s.GetLog(500, &got); err != nil || string(got.Data) != "direct-payload" {
		t.Fatalf("GetLog after direct+bind: %q, %v", got.Data, err)
	}
	// 重启后依旧单副本可读
	s.Close()
	s2, err := Open(filepath.Join(dir, "logs"), refOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var got2 raft.Log
	if err := s2.GetLog(500, &got2); err != nil || string(got2.Data) != "direct-payload" {
		t.Fatalf("post-restart GetLog: %q, %v", got2.Data, err)
	}
}

// J5: 直发接受/绑定计数 — Δbind/Δt = 绑定 segs/s, 多 raft 组立项判据的数据面。
func TestWriteRedoDirectBindCounters(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), refOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// 3 条直发: direct_segs=3, bind=0, unbound 水位上升
	for i := uint64(1); i <= 3; i++ {
		if err := s.WriteRedoDirect(0, i*1000, []byte("j5-payload")); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.RedoDirectSegs != 3 || st.RedoBoundSegs != 0 {
		t.Fatalf("after 3 direct: direct=%d bound=%d, want 3/0", st.RedoDirectSegs, st.RedoBoundSegs)
	}
	if st.RedoUnboundBytes <= 0 {
		t.Fatalf("unbound water level must rise, got %d", st.RedoUnboundBytes)
	}
	// raft 绑定 1 条: bound_segs=1, unbound 恰好回落一条记录的量
	before := s.Stats().RedoUnboundBytes
	if err := s.StoreLog(&raft.Log{Index: 1000, Term: 1, Type: raft.LogCommand, Data: []byte("j5-payload")}); err != nil {
		t.Fatal(err)
	}
	st = s.Stats()
	if st.RedoDirectSegs != 3 || st.RedoBoundSegs != 1 {
		t.Fatalf("after 1 bind: direct=%d bound=%d, want 3/1", st.RedoDirectSegs, st.RedoBoundSegs)
	}
	if dropped := before - st.RedoUnboundBytes; dropped != st.RedoBoundBytes {
		t.Fatalf("unbound drop %d != bound bytes %d (口径不一致)", dropped, st.RedoBoundBytes)
	}
	// raft 路径写的段(无直发)也算绑定: IndexLSNCodec lsn=index=1001
	// (索引必须连续; 1001 未被直发命中 → 走 wrote 路径建新段)
	if err := s.StoreLog(&raft.Log{Index: 1001, Term: 1, Type: raft.LogCommand, Data: []byte("raft-path-payload")}); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.RedoBoundSegs != 2 {
		t.Fatalf("raft-path seg must count as bound: %d", st.RedoBoundSegs)
	}
}

func TestWriteRedoDirectBackpressure(t *testing.T) {
	dir := t.TempDir()
	opts := refOpts()
	opts.UnboundRedoLimit = 4096
	s, err := Open(filepath.Join(dir, "logs"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	payload := bytes.Repeat([]byte("x"), 1024)
	for i := uint64(1); i <= 3; i++ {
		if err := s.WriteRedoDirect(0, i*1000, payload); err != nil {
			t.Fatalf("direct %d: %v", i, err)
		}
	}
	// 3×(1024+24+4+pad) ≈ 3×1536=4608 > 4096 → 第 4 条应被背压
	if err := s.WriteRedoDirect(0, 4000, payload); err != ErrUnboundFull {
		t.Fatalf("want ErrUnboundFull, got %v", err)
	}
	// raft 绑定一条 → 未绑定水位回落 → 可再写。
	// refOpts 用 IndexLSNCodec(lsn=index): Index=1000 的条目引用 redoMap[1000]。
	if err := s.StoreLog(&raft.Log{Index: 1000, Term: 1, Type: raft.LogCommand, Data: payload}); err != nil {
		t.Fatal(err)
	}
	if s.unboundBytes == 0 {
		t.Fatal("unbound should still have 2 direct segments pending")
	}
	if err := s.WriteRedoDirect(0, 5000, payload); err != nil {
		t.Fatalf("after bind, direct write should fit: %v", err)
	}
}

func TestWriteRedoDirectNonRef(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{}) // legacy
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.WriteRedoDirect(0, 1, []byte("x")); err != ErrNotRefMode {
		t.Fatalf("want ErrNotRefMode, got %v", err)
	}
}

// J5 泄漏修复(9.24)回归: InnoDB 块渐满重发模式(同 LSN 多代, 逐代变长)。
// 旧实现: 每代 reserve, 只有绑定侧 exact-match 按最新代长度扣一次 —— 旧代
// 预留 60% 永久泄漏, 水位单调涨满 UnboundRedoLimit → 直发通道冻结(K6 实测
// unbound 钉死 268435456 = 2^28)。修复后: 绑定=终点事件整条清账, 水位归零。
func TestWriteRedoDirectMultiGenLeak(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), refOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const lsn = uint64(3000)
	// 三代重发: 512B 块随填充重写, 逐代变长(更新快照路径)。
	gens := [][]byte{
		bytes.Repeat([]byte("a"), 100),
		bytes.Repeat([]byte("b"), 300),
		bytes.Repeat([]byte("c"), 500),
	}
	var peak int64
	for i, g := range gens {
		if err := s.WriteRedoDirect(0, lsn, g); err != nil {
			t.Fatalf("gen %d: %v", i, err)
		}
		if after := s.Stats().RedoUnboundBytes; after > peak {
			peak = after
		}
	}
	// 只有最新一代真实占盘; 旧两代应已被重写路径清账。台账口径 = 记录全长
	// (recLen = 4 + frame(24B) + payload), 从 redoMap 取实际值。
	st := s.Stats()
	lastGen := gens[2]
	wantLen := int64(s.redoMap[lsn].length)
	if st.RedoUnboundBytes != wantLen {
		t.Fatalf("unbound after 3 gens = %d, want latest gen record %d (旧代残账泄漏)", st.RedoUnboundBytes, wantLen)
	}
	if peak > wantLen {
		t.Fatalf("interim peak %d exceeded latest gen record %d", peak, wantLen)
	}
	// 绑定: 终点事件整条清账 → 水位归零; bound bytes = 最新代记录全长。
	if err := s.StoreLog(&raft.Log{Index: 3000, Term: 1, Type: raft.LogCommand, Data: lastGen}); err != nil {
		t.Fatal(err)
	}
	st = s.Stats()
	if st.RedoUnboundBytes != 0 {
		t.Fatalf("unbound after bind = %d, want 0", st.RedoUnboundBytes)
	}
	if got := st.RedoBoundBytes; got != wantLen {
		t.Fatalf("bound bytes = %d, want %d (最新代记录全长)", got, wantLen)
	}
	// 防双扣: 已绑定 LSN 的重发(直发 absorb + raft 重投)不得使现金变负或动台账。
	if err := s.WriteRedoDirect(0, lsn, lastGen); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.RedoUnboundBytes != 0 {
		t.Fatalf("post-bind re-delivery moved unbound: %d", st.RedoUnboundBytes)
	}
	// 现金=Σ台账 不变式(内部字段直接核)
	var sum int64
	for _, p := range s.unboundPending {
		sum += p
	}
	if sum != s.unboundBytes {
		t.Fatalf("invariant broken: cash %d != Σledger %d", s.unboundBytes, sum)
	}
	// 负残留双扣(旧版无 !ok 闸门时, 重发会使现金被重复扣减)
	if s.unboundBytes < 0 {
		t.Fatalf("negative cash: %d", s.unboundBytes)
	}
}

// TestWriteRedoDirectUnboundReloadRebuild: 重启后台账从 redo 扫描重建 ——
// 未绑定直发残留(崩溃时未及绑定)重新挂账, 已绑定者不入账。
func TestWriteRedoDirectUnboundReloadRebuild(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, refOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteRedoDirect(0, 4001, []byte("unbound-on-crash")); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLog(&raft.Log{Index: 4002, Term: 1, Type: raft.LogCommand, Data: []byte("bound-payload")}); err != nil {
		t.Fatal(err)
	}
	before := s.Stats().RedoUnboundBytes
	if before <= 0 {
		t.Fatalf("pre-restart unbound must be >0, got %d", before)
	}
	s.Close()
	s2, err := Open(lp, refOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	after := s2.Stats().RedoUnboundBytes
	if after != before {
		t.Fatalf("reloaded unbound = %d, want %d (台账从 redo 重建)", after, before)
	}
	// 重建的台账仍可被绑定清账(残余段最终归宿正确)
	if err := s2.StoreLog(&raft.Log{Index: 4003, Term: 1, Type: raft.LogCommand, Data: []byte("unbound-on-crash")}); err != nil {
		t.Fatal(err)
	}
	// 4003 的 lsn != 4001, 不会命中 —— 用 ReadRedo 确认未绑定字节仍在,
	// 由业务侧绑定路径另行处理; 此处只断言重建值恒等。
	if _, ok := s2.ReadRedo(4001); !ok {
		t.Fatal("unbound direct bytes lost on reload")
	}
}

// TestWriteRedoDirectCrashSurvival: 直发未绑定字节崩溃后仍在(P4 故障切换红利:
// 新主可直接绑定, 无需重发数据)。
func TestWriteRedoDirectCrashSurvival(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, refOpts())
	if err != nil {
		t.Fatal(err)
	}
	s.WriteRedoDirect(0, 777, []byte("pre-crash-direct"))
	s.Close() // 干净关闭, 未绑定字节也在盘上
	s2, err := Open(lp, refOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, ok := s2.redoMap[777]; !ok {
		t.Fatal("direct-written bytes lost on restart")
	}
	// 新 term 的 raft 条目直接绑定它(幂等)
	if err := s2.StoreLog(&raft.Log{Index: 1, Term: 2, Type: raft.LogCommand, Data: []byte("pre-crash-direct")}); err != nil {
		t.Fatal(err)
	}
	// IndexLSNCodec: lsn=index=1 ≠ 777 —— 这条 StoreLogs 不会复用 777(不同 lsn)。
	// 直接验证 777 仍独立可读:
	if p, ok := s2.ReadRedo(777); !ok || string(p) != "pre-crash-direct" {
		t.Fatalf("ReadRedo(777) = %q,%v", p, ok)
	}
}

// ---------------------------------------------------------------------------
// P4: ElideLogData / ResolveElidedData

func TestElideResolveRoundtrip(t *testing.T) {
	dir := t.TempDir()
	// leader 侧 store(ref + codec)
	leader, err := Open(filepath.Join(dir, "leader"), Options{Mode: ModeRef, Codec: batchTestCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	// follower 侧 store
	follower, err := Open(filepath.Join(dir, "follower"), Options{Mode: ModeRef, Codec: batchTestCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	big := bytes.Repeat([]byte("D"), 300)
	entry := mkBatchEntry(42, RedoSegment{LSN: 9001, Payload: big}, RedoSegment{LSN: 9002, Payload: []byte("seg2")})
	elided, ok := leader.ElideLogData(entry)
	if !ok {
		t.Fatal("entry not elided")
	}
	if elided.Data != nil || len(elided.Extensions) == 0 {
		t.Fatalf("elided entry malformed: data=%v ext=%d", elided.Data, len(elided.Extensions))
	}
	m := raft.DecodeElideMarker(elided.Extensions)
	if m == nil || len(m.LSNs) != 2 || m.LSNs[0] != 9001 {
		t.Fatalf("marker: %+v", m)
	}
	// follower 有字节(直发) → 解析回原始 Data
	follower.WriteRedoDirect(m.Base, 9001, big)
	follower.WriteRedoDirect(m.Base, 9002, []byte("seg2"))
	data, err := follower.ResolveElidedData(elided.Extensions)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, entry.Data) {
		t.Fatalf("resolved data mismatch: %d vs %d bytes", len(data), len(entry.Data))
	}
	// 缺字节 follower → RedoDataMissingError
	needy, _ := Open(filepath.Join(dir, "needy"), Options{Mode: ModeRef, Codec: batchTestCodec{}})
	defer needy.Close()
	if _, err := needy.ResolveElidedData(elided.Extensions); err == nil {
		t.Fatal("missing bytes must error")
	} else {
		var dm *raft.RedoDataMissingError
		if !errors.As(err, &dm) {
			t.Fatalf("want RedoDataMissingError, got %T %v", err, err)
		}
	}
	// 小负载不省略
	small := mkBatchEntry(43, RedoSegment{LSN: 9003, Payload: []byte("tiny")})
	if _, ok := leader.ElideLogData(small); ok {
		t.Fatal("tiny entry should not be elided")
	}
	// 非 redo 条目不省略
	if _, ok := leader.ElideLogData(&raft.Log{Index: 44, Term: 1, Type: raft.LogConfiguration, Data: []byte("config-bytes-long-enough-to-matter-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")}); ok {
		t.Fatal("non-redo entry must not be elided")
	}
}

// ---------------------------------------------------------------------------
// P3+P4 集成: 3 节点 inmem raft 集群, ref stores, 指针级复制

// TestRaftRefPointerTier: leader Apply redo 条目; follower 经直发已有字节 →
// AppendEntries 指针档(线上无 Data) → follower 本地解析存储 → FSM 收到完整 Data。
// 缺字节的 follower 经 DataMissingFrom 回退全量。
func TestRaftRefPointerTier(t *testing.T) {
	const n = 3
	dirs := make([]string, n)
	stores := make([]*Store, n)
	stables := make([]*StableStore, n)
	rafts := make([]*raft.Raft, n)
	trans := make([]*raft.InmemTransport, n)
	fsms := make([]*mockFSM, n)
	addrs := make([]raft.ServerAddress, n)

	for i := 0; i < n; i++ {
		dirs[i] = t.TempDir()
		var err error
		stores[i], err = Open(dirs[i], Options{Mode: ModeRef, Codec: batchTestCodec{}})
		if err != nil {
			t.Fatal(err)
		}
		stables[i], err = OpenStable(dirs[i])
		if err != nil {
			t.Fatal(err)
		}
		addrs[i], trans[i] = raft.NewInmemTransport("")
		fsms[i] = &mockFSM{}
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				trans[i].Connect(addrs[j], trans[j])
			}
		}
	}
	snap := func(i int) raft.SnapshotStore {
		s, err := raft.NewFileSnapshotStore(dirs[i], 1, os.Stderr)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	var servers []raft.Server
	for i := 0; i < n; i++ {
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(fmt.Sprintf("n%d", i)), Address: addrs[i]})
	}
	for i := 0; i < n; i++ {
		cfg := raft.DefaultConfig()
		cfg.LocalID = servers[i].ID
		cfg.HeartbeatTimeout = 150 * time.Millisecond
		cfg.ElectionTimeout = 150 * time.Millisecond
		cfg.CommitTimeout = 20 * time.Millisecond
		cfg.LeaderLeaseTimeout = 100 * time.Millisecond
		cfg.RefReplicationEnabled = true
		cfg.MaxAppendEntries = 16
		cfg.LogLevel = "ERROR"
		r, err := raft.NewRaft(cfg, fsms[i], stores[i], stables[i], snap(i), trans[i])
		if err != nil {
			t.Fatal(err)
		}
		rafts[i] = r
	}
	defer func() {
		for i := 0; i < n; i++ {
			rafts[i].Shutdown()
			stores[i].Close()
		}
	}()
	if err := rafts[0].BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatal(err)
	}
	// 等选主
	var leader *raft.Raft
	for dl := time.Now().Add(8 * time.Second); time.Now().Before(dl); {
		for _, r := range rafts {
			if r.State() == raft.Leader {
				leader = r
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	payload := append([]byte("MARKER-7001-"), bytes.Repeat([]byte("p"), 500)...)
	entryData := batchTestCodec{}.MergeSegments(0, []RedoSegment{{LSN: 7001, Payload: payload}})
	// P3 直发: 先把字节写到全部 follower 的 redo.log(绕过 raft)
	for i := 1; i < n; i++ {
		if err := stores[i].WriteRedoDirect(0, 7001, payload); err != nil {
			t.Fatal(err)
		}
	}
	f := leader.Apply(entryData, 5*time.Second)
	if err := f.Error(); err != nil {
		t.Fatal(err)
	}
	// 等全部 follower 应用
	time.Sleep(2 * time.Second)
	for i := 1; i < n; i++ {
		applied := fsms[i].appliedLogs()
		if len(applied) == 0 {
			t.Fatalf("follower %d FSM got nothing", i)
		}
		got := applied[len(applied)-1]
		if !bytes.Equal(got.Data, entryData) {
			t.Fatalf("follower %d FSM data mismatch (%d vs %d bytes)", i, len(got.Data), len(entryData))
		}
		// 单副本证据: follower 的 raft.log(meta) 不含负载; redo.log 恰好一份
		meta, _ := os.ReadFile(filepath.Join(dirs[i], "raft.log"))
		if bytes.Contains(meta, []byte("MARKER-7001-")) {
			t.Fatalf("follower %d meta file contains payload (not pointer form)", i)
		}
		redo, _ := os.ReadFile(filepath.Join(dirs[i], redoFileName))
		if c := bytes.Count(redo, []byte("MARKER-7001-")); c != 1 {
			t.Fatalf("follower %d redo payload copies = %d, want 1", i, c)
		}
	}

	// 回退路径: 新条目, follower-2 缺字节(不直发给它)
	p2 := append([]byte("MARKER-8001-"), bytes.Repeat([]byte("q"), 588)...)
	e2 := batchTestCodec{}.MergeSegments(0, []RedoSegment{{LSN: 8001, Payload: p2}})
	stores[1].WriteRedoDirect(0, 8001, p2) // follower-1 有
	// follower-2 没有 → 期望 leader 回退全量后其也收敛
	f2 := leader.Apply(e2, 5*time.Second)
	if err := f2.Error(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	for i := 1; i < n; i++ {
		found := false
		for _, l := range fsms[i].appliedLogs() {
			if bytes.Equal(l.Data, e2) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("follower %d never converged on the fallback entry", i)
		}
	}
}

// mockFSM: minimal raft.FSM recording applied logs.
type mockFSM struct {
	mu   sync.Mutex
	logs []*raft.Log
}

func (m *mockFSM) Apply(l *raft.Log) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, l)
	return nil
}
// appliedLogs 返回已应用日志快照(读方与 raft FSM goroutine 的 Apply 并发,
// 必须经同一把 mu —— 直接读 m.logs 字段会被 -race 抓到)。
func (m *mockFSM) appliedLogs() []*raft.Log {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*raft.Log(nil), m.logs...)
}

func (m *mockFSM) Snapshot() (raft.FSMSnapshot, error) { return &mockSnap{}, nil }
func (m *mockFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

type mockSnap struct{}

func (s *mockSnap) Persist(raft.SnapshotSink) error { return nil }
func (s *mockSnap) Release()                        {}
