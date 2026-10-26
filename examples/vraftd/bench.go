// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// benchReq describes an in-process load run: total applies, client concurrency
// and the value payload size in bytes. Set ops to N to spread over clients.
type benchReq struct {
	Ops       int `json:"n"`    // total apply calls
	Clients   int `json:"c"`    // concurrent client goroutines
	Size      int `json:"size"` // value size in bytes
	TimeoutMs int `json:"timeout_ms"`
}

// benchResult reports throughput and the latency distribution.
type benchResult struct {
	Ops        int     `json:"ops"`
	Failed     int     `json:"failed"`
	DurationNs int64   `json:"duration_ns"`
	Throughput float64 `json:"throughput_ops_s"`
	P50Ns      int64   `json:"p50_ns"`
	P90Ns      int64   `json:"p90_ns"`
	P99Ns      int64   `json:"p99_ns"`
	MaxNs      int64   `json:"max_ns"`
	Applied    uint64  `json:"fsm_applied_total"`
}

// runBench 在进程内起 C 个并发写客户端跑完 N 次 Apply, 返回吞吐与延迟分位。
// runBench drives N concurrent writers each submitting raft.Apply and waiting
// for the commit quorum, then returns throughput and latency percentiles.
// The writes go through the full vraft write path: BatchWindow group-commit
// (if enabled) and async leader persist (if enabled).
//
// 算法解读：
//
//	原理：每个 client goroutine 串行地 r.Apply 再 f.Error() 等提交, 总条数按
//	      per=Ops/Clients 加余数分摊; 单条延迟取 t0 → f.Error() 返回的墙钟差,
//	      也就是"入队 → leader 攒批 → 复制到 quorum → FSM 应用"的端到端提交
//	      延迟, 而不是单次磁盘写的延迟。吞吐是 Ops 除以整段墙钟时间。
//	关键不变量：必须跑在 leader 上——非 leader 的 r.Apply 立即返回 ErrNotLeader,
//	      测出来的是错误路径; 因此前置检查 r.State() != raft.Leader 直接报错。
//	      所有 client 共享同一个 *raft.Raft 实例, 所以"C 个 client"衡量的是**在途
//	      Apply 数**(组提交窗口里的批有多大), 不是 TCP 连接数。
//	代价与边界：吞吐的分子用的是 req.Ops(含失败条目), 分母是墙钟时间, 所以
//	      Failed>0 时吞吐被系统性高估; 而延迟样本只收成功条目(失败走 continue
//	      直接跳过), 因此 p99 反映的是成功路径的尾延迟, 两者口径并不完全一致。
//	      lat 全量保留所有样本, 百万级 op 会占用几十 MB, 末尾那次 sort 本身就是
//	      可观开销(会把吞吐略微拉低)。
//	设计权衡：报 p50/p99 而不是均值, 是因为均值会被少数长尾(组提交窗口到期、
//	      fsync 抖动、GC)拉偏; p50 代表典型延迟, p99 代表最差 1% 的体验, 再单独
//	      给 Max 是因为样本少时 p99 会直接退化成 Max, 只有两者都看才分得清
//	      "整体变慢"还是"尾巴变长"。
func runBench(r *raft.Raft, fsm *KVFSM, req benchReq) (*benchResult, error) {
	if req.Ops <= 0 {
		return nil, errors.New("n (total ops) must be positive")
	}
	if req.Clients <= 0 {
		return nil, errors.New("c (clients) must be positive")
	}
	if req.Size < 0 {
		return nil, errors.New("size must be non-negative")
	}
	if r.State() != raft.Leader {
		return nil, errors.New("bench must run against the leader (call /bench on the leader)")
	}

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		// 30s 是演示用的"不会误伤"值: 足以覆盖 leader 选举(默认 election 1s)
		// 加一次快照, 又短到能在卡死时让基准自己结束而不是永久挂住。
		timeout = 30 * time.Second
	}

	per := req.Ops / req.Clients
	rem := req.Ops % req.Clients
	// 余数分给前 rem 个 client, 保证 sum(m) == req.Ops(见下面 c < rem 的判断)。
	val := strings.Repeat("x", req.Size)

	var (
		mu     sync.Mutex
		lat    []time.Duration
		failed int64
		wg     sync.WaitGroup
	)
	start := time.Now()
	for c := 0; c < req.Clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			m := per
			if c < rem {
				m++
			}
			for i := 0; i < m; i++ {
				key := fmt.Sprintf("bench-%d-%d", c, i)
				cmd, _ := json.Marshal(kvCmd{Op: "set", K: key, V: val})
				t0 := time.Now()
				f := r.Apply(cmd, timeout)
				if err := f.Error(); err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				d := time.Since(t0)
				// lat 只被这把 mu 保护, 且只在 wg.Wait() 之后才被读, 所以排序
				// 时不需要再加锁; 失败条目在上面 continue 掉了, 不进延迟样本。
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := &benchResult{
		Ops:        req.Ops,
		Failed:     int(atomic.LoadInt64(&failed)),
		DurationNs: elapsed.Nanoseconds(),
		Throughput: float64(req.Ops) / elapsed.Seconds(),
		Applied:    fsm.Applied(),
	}
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		res.P50Ns = percentile(lat, 50).Nanoseconds()
		res.P90Ns = percentile(lat, 90).Nanoseconds()
		res.P99Ns = percentile(lat, 99).Nanoseconds()
		res.MaxNs = lat[len(lat)-1].Nanoseconds()
	}
	return res, nil
}

// readBenchReq describes an in-process read-load run: total reads, client
// concurrency and the consistency tier (strict|lease|stale).
type readBenchReq struct {
	Ops         int    `json:"n"`
	Clients     int    `json:"c"`
	Consistency string `json:"consistency"`
}

// readBenchResult reports throughput and the latency distribution of a single
// read tier. Latency units match the write bench (nanoseconds); the driver
// converts to ms for display.
type readBenchResult struct {
	Consistency string  `json:"consistency"`
	Ops         int     `json:"ops"`
	Failed      int     `json:"failed"`
	DurationNs  int64   `json:"duration_ns"`
	Throughput  float64 `json:"throughput_ops_s"`
	P50Ns       int64   `json:"p50_ns"`
	P90Ns       int64   `json:"p90_ns"`
	P99Ns       int64   `json:"p99_ns"`
	MaxNs       int64   `json:"max_ns"`
	MinNs       int64   `json:"min_ns"`
}

// runReadBench 在进程内测一个读一致性档位: C 个 client 各自 VerifyReadIndex(tier)
// 再读同一个固定键, 报出吞吐与延迟分位。
// runReadBench measures one read tier (12.2) in-process: C client goroutines
// each call VerifyReadIndex(tier) and then read a fixed key from the FSM. This
// is the same access pattern as /read?consistency=..., so strict pays a quorum
// round per read while lease/stale are local and only differ in the
// leadership/lease check. The tier must be satisfiable by this node: strict and
// lease require the leader (lease falls back to strict when not held).
//
// 算法解读：
//
//	原理：每个 client 循环"抢一条配额 → VerifyReadIndex → 从 FSM 读键 → 记延迟"。
//	      配额用 atomic.AddInt32(&remain, -1) 领取: 从 Ops 递减到 0 的那 Ops 次调用
//	      拿到非负值并继续, 第 Ops+1 次拿到 -1 就退出, 因此总执行次数**恰好**是 Ops,
//	      且各 client 之间不需要额外的分发逻辑(谁快谁多干)。
//	关键不变量：三个档位的代价差在 VerifyReadIndex 里——strict 每读一次都要一次
//	      quorum 往返确认自己还是 leader, lease 只在租约有效时走本地快路径、否则
//	      回落 strict, stale 只取本地 applied 水位。所以这个基准测出的档位间差距,
//	      本质上是"读要不要付一次网络往返"。seed 那一步必须等 raft 的
//	      AppliedIndex 越过 seed 的 index(而不是等 FSM 计数), 否则下面的读可能
//	      读不到键而把 failed 记成错误。
//	代价与边界：与 runBench 不同, 这里**失败也进延迟样本**(先记 d 再判 err), 所以
//	      failed>0 时 p99 混入了失败路径的耗时, 两处口径不一致。remain 用 int32,
//	      Ops 超过 2^31 会回绕。
//	设计权衡：让所有 client 读同一个键而不是分散到多个键, 是为了把变量收敛到
//	      "读一致性开销"本身; 代价是命中同一个 map 槽位, 缓存行为比真实负载更友好。
func runReadBench(r *raft.Raft, fsm *KVFSM, req readBenchReq) (*readBenchResult, error) {
	if req.Ops <= 0 {
		return nil, errors.New("n (total ops) must be positive")
	}
	if req.Clients <= 0 {
		return nil, errors.New("c (clients) must be positive")
	}
	var tier raft.ReadConsistency
	switch req.Consistency {
	case "", "strict":
		tier = raft.ReadStrict
	case "lease":
		tier = raft.ReadLease
	case "stale":
		tier = raft.ReadStale
	default:
		return nil, fmt.Errorf("unknown consistency %q (want strict|lease|stale)", req.Consistency)
	}

	// Seed a key through raft and wait until the FSM has applied it, so every
	// read below finds a value.
	seedKey := fmt.Sprintf("bench-read-%d", time.Now().UnixNano())
	// 64 字节的种子值只是让"读到空串"和"没读到"可区分, 不参与任何测量。
	cmd, _ := json.Marshal(kvCmd{Op: "set", K: seedKey, V: strings.Repeat("x", 64)})
	seedFut := r.Apply(cmd, 10*time.Second)
	if err := seedFut.Error(); err != nil {
		return nil, fmt.Errorf("seed apply: %w", err)
	}
	// Wait for raft to apply the seed entry (raft applied index, not the FSM
	// command counter — see handleRead for why the two diverge).
	// 1ms 轮询是刻意的粗粒度: 这段只在基准开始前跑一次, 用它换掉一个条件变量
	// 的复杂度很划算; 10s 上限则保证 FSM 卡住时基准会报错而不是永久阻塞。
	deadline := time.Now().Add(10 * time.Second)
	for r.AppliedIndex() < seedFut.Index() {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("fsm never applied seed at index %d", seedFut.Index())
		}
		time.Sleep(time.Millisecond)
	}

	var (
		mu     sync.Mutex
		lat    []time.Duration
		failed int64
		wg     sync.WaitGroup
		remain int32
	)
	remain = int32(req.Ops)
	start := time.Now()
	for c := 0; c < req.Clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// 原子领配额: 拿到负数即"活干完了", 直接退出; 非负的那个值本身
				// 没人用, 它只是让 Add 的返回值同时充当计数器与信号。
				if atomic.AddInt32(&remain, -1) < 0 {
					return
				}
				t0 := time.Now()
				_, err := r.VerifyReadIndex(tier)
				if err == nil {
					if _, ok := fsm.Get(seedKey); !ok {
						err = fmt.Errorf("seeded value missing from FSM")
					}
				}
				d := time.Since(t0)
				// 注意: 这里失败也记进延迟样本(与 runBench 不同), 所以 failed>0 时
				// 分位数里混着失败路径的耗时。
				if err != nil {
					atomic.AddInt64(&failed, 1)
				}
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := &readBenchResult{
		Consistency: tier.String(),
		Ops:         req.Ops,
		Failed:      int(atomic.LoadInt64(&failed)),
		DurationNs:  elapsed.Nanoseconds(),
		Throughput:  float64(req.Ops) / elapsed.Seconds(),
	}
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		res.P50Ns = percentile(lat, 50).Nanoseconds()
		res.P90Ns = percentile(lat, 90).Nanoseconds()
		res.P99Ns = percentile(lat, 99).Nanoseconds()
		res.MaxNs = lat[len(lat)-1].Nanoseconds()
		res.MinNs = lat[0].Nanoseconds()
	}
	return res, nil
}

// percentile 在**已升序**的样本里取第 p 百分位, 采用离散秩(不做插值)。
//
// 算法解读：
//
//	原理：idx = floor(len * p / 100), 直接返回该下标处的实测值。于是 p=50 且
//	      len=100 时取的是第 51 小的元素(下标 50), 而不是中位两数的平均。
//	关键不变量：调用方必须保证 sorted 非空且已排序——空切片会让 idx = -1 并在
//	      下一行 panic, 本函数自己不校验。runBench/runReadBench 都用 len(lat) > 0
//	      守住这个前提。
//	代价与边界：取值本身 O(1), 但前提 O(n log n) 的排序已经付过了; 因为不插值,
//	      样本很少时 p50 与 p99 可能落在同一个元素上, 此时两个数会完全相等, 不能
//	      当成"分布很集中"的证据。p=100 时 idx 会等于 len, 必须靠下面的夹取兜住。
//	设计权衡：不引统计库、不做线性插值, 是因为基准要的是"第 N 百分位的实测值"
//	      这种可解释、可复现的数, 而不是一个平滑出来的估计值。
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		// 仅 p=100 会走到这里; 夹到最后一个元素, 于是 percentile(...,100) == Max。
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
