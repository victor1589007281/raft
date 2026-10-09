package raft

// #105-B 端到端: 领导者遇到**包装过的** ErrLogNotFound 时, 必须走快照回退(SEND_SNAP)。
//
// 修前 `replicateTo` 用身份比较 `err == ErrLogNotFound`; filestore 的**空洞**分支返回的是
// `fmt.Errorf(..., %w, ErrLogNotFound)`, 身份比较为假 ⇒ 只走到 `return`, SEND_SNAP **永不可达**。
// 领导者在同一个取不到的索引上每轮 fail-loud 重试, 既不自愈也不出声, 调用侧在 commit 上等死。
//
// 本用例把空洞的**error 形态**(而不是造一个真洞)注入成 LogStore 的一层包装, 用**真集群**
// 走一遍 replicateTo, 读回两件事:
//   ① 领导者确实尝试了快照回退(修前: 一次都没有);
//   ② 复制确实卡在空洞上(跟随者停在空洞之下 —— 这正是现场"commit 冻结"的形态)。

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

// syncLogBuf 线程安全的日志收集器(hclog 从多个 goroutine 写)。
type syncLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// holeLogStore 只在 GetLog 的一个索引上改行为, 返回与 filestore 空洞分支**逐字同形**的包装 error;
// 其余方法全部透传(尤其 StoreLogs —— 条目确实写进去了, 只是**读不回来**, 与真实现场一致:
// 领导者手里有 18716+, 却读不到 18714)。
type holeLogStore struct {
	LogStore
	hole atomic.Uint64 // 0 = 未武装
}

func (h *holeLogStore) GetLog(index uint64, l *Log) error {
	if hv := h.hole.Load(); hv != 0 && index == hv {
		return fmt.Errorf("filestore: hole detected: GetLog(%d) maps to entry 0 (first=%d len=0): %w",
			index, index+1, ErrLogNotFound)
	}
	return h.LogStore.GetLog(index, l)
}

// TestReplicateToFallsBackToSnapshotOnWrappedHole 真二节点集群 + 包装空洞。
func TestReplicateToFallsBackToSnapshotOnWrappedHole(t *testing.T) {
	var logs syncLogBuf
	conf := DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "hole-test",
		Level:  hclog.Trace,
		Output: &logs,
	})

	// 手搭一个 2 节点集群 —— 唯一目的是能在 **NewRaft 构造时**就把 holeLogStore 包进去。
	// (MakeCluster 内部自建 InmemStore, 事后替换 r.logs 会与复制 goroutine 数据竞争。)
	const peers = 2
	var (
		addrs  []ServerAddress
		transs []*InmemTransport
		stores []*holeLogStore
		rafts  []*Raft
		fsms   []*MockFSM
		conf2  Configuration
	)
	for i := 0; i < peers; i++ {
		addr, trans := NewInmemTransport("")
		addrs = append(addrs, addr)
		transs = append(transs, trans)
		conf2.Servers = append(conf2.Servers, Server{
			Suffrage: Voter,
			ID:       ServerID(fmt.Sprintf("hole-%d", i)),
			Address:  addr,
		})
	}
	for i, j := range []int{1, 0} {
		transs[i].Connect(addrs[j], transs[j])
	}
	for i := 0; i < peers; i++ {
		_, snap := FileSnapTest(t)
		stores = append(stores, &holeLogStore{LogStore: NewInmemStore()})
		fsms = append(fsms, &MockFSM{})
		pc := *conf // 每个节点一份独立副本(不能就地改共享 conf)
		pc.LocalID = conf2.Servers[i].ID
		pc.Logger = conf.Logger
		if err := BootstrapCluster(&pc, stores[i], NewInmemStore(), snap, transs[i], conf2); err != nil {
			t.Fatalf("BootstrapCluster(%d): %v", i, err)
		}
		r, err := NewRaft(&pc, fsms[i], stores[i], NewInmemStore(), snap, transs[i])
		if err != nil {
			t.Fatalf("NewRaft(%d): %v", i, err)
		}
		rafts = append(rafts, r)
	}
	defer func() {
		for _, r := range rafts {
			_ = r.Shutdown().Error()
		}
	}()

	// 选出 leader。
	var leader *Raft
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for _, r := range rafts {
			if r.State() == Leader {
				leader = r
				leaders++
			}
		}
		if leaders == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("5s 内没选出唯一 leader")
	}
	li := -1
	for i, r := range rafts {
		if r == leader {
			li = i
		}
	}

	// 提交 3 条(索引 2..4; 索引 1 是 Bootstrap 的配置条目), 让跟随者追平。
	for n := 0; n < 3; n++ {
		if err := leader.Apply([]byte("warm"), 2*time.Second).Error(); err != nil {
			t.Fatalf("预热提交 %d 失败: %v", n, err)
		}
	}
	lastBefore, _ := leader.getLastLog()
	fLastBefore, _ := rafts[1-li].getLastLog()
	if fLastBefore != lastBefore {
		t.Fatalf("预热后未追平: leader=%d follower=%d", lastBefore, fLastBefore)
	}
	t.Logf("预热完成: 双方 lastIndex=%d", lastBefore)

	// 武装空洞: 下一条(索引 lastBefore+1)读不回来。
	hole := lastBefore + 1
	stores[li].hole.Store(hole)

	// 提交一条 —— 领导者本地写得进去, 但复制时读不回索引 hole。
	//
	// ★刻意**不**等 fut.Error(): ApplyLog 的 timeout 只界住「入队」, 不界住「FSM 应用」。
	// 这里复制正卡在空洞上 ⇒ 该条永远 apply 不了 ⇒ .Error() 会是**无界阻塞**。
	// (这一点本身也是现场"调用侧等死"的同构: 上层等的是一个永远不会到来的完成。)
	start := time.Now()
	_ = leader.Apply([]byte("trigger"), 300*time.Millisecond)

	// 给复制循环几轮机会去撞这个洞。
	snapDeadline := time.Now().Add(4 * time.Second)
	sawSnapshot := false
	for time.Now().Before(snapDeadline) {
		s := logs.String()
		if strings.Contains(s, "failed to send snapshot to") || strings.Contains(s, "opening snapshot") {
			sawSnapshot = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	fLast, _ := rafts[1-li].getLastLog()
	t.Logf("空洞 armed=%d; 等待 %v; 跟随者 lastIndex=%d (空洞之下=%v)",
		hole, time.Since(start).Round(time.Millisecond), fLast, fLast < hole)

	if !sawSnapshot {
		t.Fatalf("#105-B: 领导者从未尝试快照回退 —— 包装过的 ErrLogNotFound 没能到达 SEND_SNAP。\n"+
			"日志里只有 fail-loud 重试:\n%s", tailLines(logs.String(), 20))
	}
	// 复制确实卡在洞上(而不是"其实复制成功了") —— 否则本用例没测到目标。
	if fLast >= hole {
		t.Fatalf("跟随者已越过空洞(%d >= %d) —— 本例没构造出阻塞, 判据无意义", fLast, hole)
	}
}

// tailLines 取末尾 n 行, 便于失败时看清领导者到底做了什么。
func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	for i := range lines {
		lines[i] = "  " + lines[i]
	}
	return strings.Join(lines, "\n")
}
