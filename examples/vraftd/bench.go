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

// runBench drives N concurrent writers each submitting raft.Apply and waiting
// for the commit quorum, then returns throughput and latency percentiles.
// The writes go through the full vraft write path: BatchWindow group-commit
// (if enabled) and async leader persist (if enabled).
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
		timeout = 30 * time.Second
	}

	per := req.Ops / req.Clients
	rem := req.Ops % req.Clients
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

// runReadBench measures one read tier (12.2) in-process: C client goroutines
// each call VerifyReadIndex(tier) and then read a fixed key from the FSM. This
// is the same access pattern as /read?consistency=..., so strict pays a quorum
// round per read while lease/stale are local and only differ in the
// leadership/lease check. The tier must be satisfiable by this node: strict and
// lease require the leader (lease falls back to strict when not held).
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
	cmd, _ := json.Marshal(kvCmd{Op: "set", K: seedKey, V: strings.Repeat("x", 64)})
	seedFut := r.Apply(cmd, 10*time.Second)
	if err := seedFut.Error(); err != nil {
		return nil, fmt.Errorf("seed apply: %w", err)
	}
	// Wait for raft to apply the seed entry (raft applied index, not the FSM
	// command counter — see handleRead for why the two diverge).
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

func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
