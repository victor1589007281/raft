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

func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
