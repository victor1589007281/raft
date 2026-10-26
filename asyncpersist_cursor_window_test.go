// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raft

// 异步持久化「游标窗口」回归测试(2026-10-24)。
//
// 窗口是什么:
//
//	persistAsyncBatch 的推进顺序是 StoreLogs(落盘) → setLastLog(内存游标) →
//	dropPendingThrough。在 StoreLogs 返回之后、setLastLog 执行之前, store 的
//	lastIndex 已经是 N, 而 raft 内存 lastLog 仍是 N-1。正常情况下 pending 缓冲
//	覆盖着 N 这一段(dispatchLogs 会取 max(lastLog, pending top)), 所以这条缝是
//	安全的。
//
//	但有两处会把这层保护拆掉, 露出「store 已到 N、内存 lastLog 仍 N-1、pending
//	已空」的状态:
//	  ① 下台清理: runLeader 的 defer 先 close(asyncWriteStop) 再 clearPending();
//	     若写者此刻正卡在 StoreLogs 里(已写完、还没 setLastLog), 它不属于本次
//	     defer 管辖, 会继续跑完 setLastLog; clearPending 却已经把 pending 清空。
//	  ② dispatchLogs 里 getLastIndex() 与 pendingMu 是两次独立取锁, 写者恰好在
//	     两次取锁之间完成 setLastLog + dropPendingThrough, 就会读到「旧 lastLog +
//	     空 pending」的组合。
//
//	两种情况下, 下一次派发若只按内存 lastLog 分配索引, 就会把已落盘的 N 再派发
//	一次 → StoreLogs 收到 batch first == store last 的重叠批(现场 40k DDL 档
//	"leader 异步持久化 first==last" 的直接来源)。
//
// 修复: 分配索引/判定 newEntries 起点时把 store 的真实 LastIndex() 也纳入
//	max。本文件的 TestAsyncPersist_CursorWindow_* 会先把状态精确钉在窗口里, 再
//	注入一次派发, 断言发给 StoreLogs 的批次从 store last+1 开始。
//
// 说明: 这里用包装 store 记录 StoreLogs 入参而不是断言 filestore 的具体报错 ——
// 根包测试无法导入 filestore(会形成 raft↔filestore 的测试导入环), 而「批次起点
// 是否越过 store last」正是本窗口的唯一判据。

import (
	"container/list"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询 cond 直到成功或超时。
func waitFor(t *testing.T, d time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", desc)
		}
		time.Sleep(time.Millisecond)
	}
}

// storeCallRecord 记录一次 StoreLogs 的边界, lastBefore 是调用前的 store last。
type storeCallRecord struct {
	first      uint64
	last       uint64
	lastBefore uint64
}

// cursorRecordingStore 包装 InmemStore: 记录每次 StoreLogs 的区间; 可选在第一次
// 落盘之后、返回之前阻塞(把异步写者精确钉在「store 已落盘、setLastLog 未执行」
// 的窗口里)。
type cursorRecordingStore struct {
	*InmemStore

	mu    sync.Mutex
	calls []storeCallRecord

	gateArmed atomic.Bool
	written   chan struct{} // 被闸住的那次 StoreLogs 已落盘(仍在其函数内)
	release   chan struct{} // 关闭后写者才从 StoreLogs 返回
}

// armGate 让下一次(且仅下一次)StoreLogs 在落盘之后、返回之前阻塞, 把异步写者
// 精确钉在「store 已到 N、内存 lastLog 仍是 N-1」的窗口里。
func (s *cursorRecordingStore) armGate() { s.gateArmed.Store(true) }

func (s *cursorRecordingStore) StoreLogs(logs []*Log) error {
	lastBefore, _ := s.InmemStore.LastIndex()
	var first, last uint64
	if len(logs) > 0 {
		first = logs[0].Index
		last = logs[len(logs)-1].Index
	}
	s.mu.Lock()
	s.calls = append(s.calls, storeCallRecord{first: first, last: last, lastBefore: lastBefore})
	s.mu.Unlock()

	// 先真正落盘, 再进入窗口
	if err := s.InmemStore.StoreLogs(logs); err != nil {
		return err
	}
	if s.written != nil && s.gateArmed.CompareAndSwap(true, false) {
		close(s.written) // 通知测试: store 已到 last, 但写者还没到 setLastLog
		<-s.release      // 测试放行后写者才继续(模拟 setLastLog)
	}
	return nil
}

func (s *cursorRecordingStore) recordedCalls() []storeCallRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]storeCallRecord, len(s.calls))
	copy(out, s.calls)
	return out
}

// newBareAsyncLeader 构造一个不跑主循环的 async leader 骨架, 只承载 dispatchLogs /
// runAsyncWriter 需要的状态(与 TestRaft_LeadershipTransferInProgress 里直接构造
// &Raft{} 的做法一致), 让测试可以精确安排线程时序。
func newBareAsyncLeader(t *testing.T, store LogStore, lastLogIndex, lastLogTerm uint64) *Raft {
	t.Helper()
	conf := DefaultConfig()
	conf.AsyncLeaderPersist = true
	conf.LocalID = "node-0"
	logger := newTestLogger(t)
	conf.Logger = logger

	r := &Raft{
		localID:         conf.LocalID,
		localAddr:       "127.0.0.1:0",
		logger:          logger,
		logs:            store,
		shutdownCh:      make(chan struct{}),
		asyncWriteCh:    make(chan asyncWrite, 8),
		leaderState:     leaderState{},
		protocolVersion: ProtocolVersionMax,
	}
	r.conf.Store(*conf)
	// 裸骨架没有 stable store; 直接写内存任期(测试只关心游标, 不关心稳定性)。
	r.raftState.setCurrentTerm(lastLogTerm)
	r.setLastLog(lastLogIndex, lastLogTerm)
	r.raftState.setState(Leader)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.commitment = newCommitment(make(chan struct{}, 1),
		Configuration{Servers: []Server{{Suffrage: Voter, ID: conf.LocalID}}},
		lastLogIndex+1, conf.LocalID)
	return r
}

func dispatchOne(r *Raft, data string) *logFuture {
	f := &logFuture{log: Log{Type: LogCommand, Data: []byte(data)}}
	r.dispatchLogs([]*logFuture{f})
	return f
}

// TestAsyncPersist_CursorWindow_NoOverlappingDispatch 复现审计给出的窗口:
// 异步写者已经把一批日志落盘, 但 setLastLog 尚未执行; 此时 pending 被下台清理
// 清空(窗口 ①)。紧接着注入一次派发。
//
//	旧代码: 新批次第一条被分配成 store 已有的 last → StoreLogs first == store last;
//	新代码: 分配 last+1, StoreLogs 严格从 store last+1 开始。
func TestAsyncPersist_CursorWindow_NoOverlappingDispatch(t *testing.T) {
	// 窗口里只挂一条在途日志: 写者按批推进游标, 若在途批有多条, lastLog
	// 会落后整批; 现场(last=15007, first=15007, len=1)是单条批, 这里照此构造。
	const firstBatch = 1
	store := &cursorRecordingStore{
		InmemStore: NewInmemStore(),
		written:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	r := newBareAsyncLeader(t, store, 0, 1)

	writerStop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		r.runAsyncWriter(writerStop)
	}()
	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer func() {
		// 失败路径也必须放行写者, 否则 cleanup 会卡在 writerDone 上。
		releaseWriter()
		close(writerStop)
		close(r.shutdownCh)
		<-writerDone
	}()

	// 预置一条已完全落盘且游标已推进的日志, 让窗口里的 lastLog 落在 N-1
	// (与现场 "batch first == store last" 的形态一致)。
	dispatchOne(r, "warm")
	waitFor(t, 5*time.Second, "warm-up batch persisted", func() bool {
		return r.getLastIndex() == 1
	})

	// 第一批(4 条)由异步写者落盘后停在 StoreLogs 里(窗口打开)。
	store.armGate()
	for i := 0; i < firstBatch; i++ {
		dispatchOne(r, "seed")
	}
	select {
	case <-store.written:
	case <-time.After(5 * time.Second):
		t.Fatal("async writer never reached the store-write window")
	}

	// 断言窗口成立: store 已到 1+firstBatch, 内存 lastLog 仍是 store last-1,
	// pending 非空(正常由它兜住这条缝)。
	storeLast, _ := store.LastIndex()
	raftLast := r.getLastIndex()
	wantStoreLast := uint64(1 + firstBatch)
	if storeLast != wantStoreLast || raftLast != wantStoreLast-1 {
		t.Fatalf("window not established: store last=%d raft lastLog=%d", storeLast, raftLast)
	}
	r.pendingMu.Lock()
	pendingLen := len(r.pendingLogs)
	r.pendingMu.Unlock()
	if pendingLen == 0 {
		t.Fatal("expected the pending buffer to cover the in-flight batch")
	}
	t.Logf("window: store last=%d, raft lastLog=%d, pending=%d (writer parked after StoreLogs)",
		storeLast, raftLast, pendingLen)

	// 模拟下台清理: runLeader 的 defer 在写者仍在 StoreLogs 内时就会 clearPending。
	r.clearPending()

	// 在窗口内注入一次派发。
	dispatchOne(r, "after-window")

	// 放行写者, 等它把两批都处理完。
	releaseWriter()
	waitFor(t, 5*time.Second, "injected batch persisted", func() bool {
		return len(store.recordedCalls()) >= 3
	})

	calls := store.recordedCalls()
	for i, c := range calls {
		if c.first != c.lastBefore+1 {
			t.Fatalf("StoreLogs call #%d starts at %d but store last is %d "+
				"(overlapping batch: first == store last); all calls=%v",
				i, c.first, c.lastBefore, calls)
		}
	}
	if got, want := calls[2].first, uint64(firstBatch+2); got != want {
		t.Fatalf("batch dispatched in the window got index %d, want %d", got, want)
	}
	// 游标应从 store 真实进度恢复(写者最终 setLastLog 到新批末尾)。
	if got, want := r.getLastIndex(), uint64(firstBatch+2); got != want {
		t.Fatalf("raft lastLog=%d, want %d after the window healed", got, want)
	}
}

// TestAsyncPersist_CursorWindow_AppendEntries 覆盖同源的 follower 侧窗口:
// 节点下台后, 旧任期的异步写者仍可能卡在 StoreLogs 里(已落盘、未 setLastLog);
// 此时收到 AppendEntries, 里面重发了 store 已有的索引(raft 只能按内存 lastLog
// 判定去重)。旧代码会把已落盘条目整段重写(first == store last)。
func TestAsyncPersist_CursorWindow_AppendEntries(t *testing.T) {
	const persisted = 6
	base := NewInmemStore()
	seed := make([]*Log, persisted)
	for i := range seed {
		seed[i] = &Log{Index: uint64(i + 1), Term: 1, Type: LogCommand, Data: []byte("seed")}
	}
	if err := base.StoreLogs(seed); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	store := &cursorRecordingStore{InmemStore: base}

	// 内存 lastLog 落后 store 一条: 模拟写者已落盘、setLastLog 未执行且 pending 已清。
	r := newBareAsyncLeader(t, store, persisted-1, 1)
	r.raftState.setState(Follower)

	addr, trans := NewInmemTransport("")
	r.trans = trans
	r.localAddr = addr

	req := &AppendEntriesRequest{
		RPCHeader:         RPCHeader{ProtocolVersion: ProtocolVersionMax, ID: []byte("leader"), Addr: []byte("peer")},
		Term:              1,
		PrevLogEntry:      persisted,
		PrevLogTerm:       1,
		LeaderCommitIndex: 0,
		Entries: []*Log{
			{Index: persisted, Term: 1, Type: LogCommand, Data: []byte("seed")},     // 已在 store 里
			{Index: persisted + 1, Term: 1, Type: LogCommand, Data: []byte("next")}, // 真正的新条目
		},
	}
	respCh := make(chan RPCResponse, 1)
	r.appendEntries(RPC{Command: req, RespChan: respCh}, req)
	resp := (<-respCh).Response.(*AppendEntriesResponse)
	if !resp.Success {
		t.Fatalf("appendEntries failed: %+v", resp)
	}

	calls := store.recordedCalls()
	if len(calls) != 1 {
		t.Fatalf("expected one StoreLogs call, got %d: %v", len(calls), calls)
	}
	if c := calls[0]; c.first != c.lastBefore+1 {
		t.Fatalf("AppendEntries re-stored an already-durable entry: first=%d store last=%d "+
			"(overlapping batch); call=%+v", c.first, c.lastBefore, c)
	}
	if got, want := calls[0].first, uint64(persisted+1); got != want {
		t.Fatalf("AppendEntries stored starting at %d, want %d", got, want)
	}
	if got, want := r.getLastIndex(), uint64(persisted+1); got != want {
		t.Fatalf("raft lastLog=%d, want %d", got, want)
	}
}
