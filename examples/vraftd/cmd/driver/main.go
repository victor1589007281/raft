// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

// Command vraft-bench is the cluster-internal benchmark driver for vraftd.
// It probes the given HTTP control-plane addresses until one reports itself
// Leader, then POSTs a /bench load run to that leader. The benchmark itself
// runs in-process inside vraftd (which is where the timers live), so the
// measured throughput and latency are exactly the raft write-path numbers.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type stateResp struct {
	Role       string `json:"role"`
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`
}

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

func getState(addr string) *stateResp {
	resp, err := http.Get("http://" + addr + "/state")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var s stateResp
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil
	}
	return &s
}

func main() {
	var (
		targets   = flag.String("targets", "", "comma-separated vraftd HTTP addresses")
		n         = flag.Int("n", 10000, "total apply ops")
		clients   = flag.Int("c", 8, "concurrent clients")
		size      = flag.Int("size", 64, "value size in bytes")
		timeoutMs = flag.Int("timeout_ms", 0, "apply timeout ms (0 = vraftd default)")
	)
	flag.Parse()
	if *targets == "" {
		fmt.Fprintln(os.Stderr, "missing -targets")
		os.Exit(2)
	}
	addrs := strings.Split(*targets, ",")

	// Wait for a leader, probing all addresses.
	var leader string
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		for _, a := range addrs {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			if s := getState(a); s != nil && s.Role == "Leader" {
				leader = a
				break
			}
		}
		if leader != "" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if leader == "" {
		fmt.Fprintln(os.Stderr, "no leader found among targets")
		os.Exit(1)
	}
	fmt.Printf("leader=%s\n", leader)

	req := map[string]int{"n": *n, "c": *clients, "size": *size, "timeout_ms": *timeoutMs}
	body, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal bench req: %v\n", err)
		os.Exit(1)
	}
	resp, err := http.Post("http://"+leader+"/bench", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "POST /bench to %s: %v\n", leader, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read /bench response: %v\n", err)
		os.Exit(1)
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "POST /bench to %s: HTTP %s: %s\n", leader, resp.Status, string(raw))
		os.Exit(1)
	}

	var res benchResult
	if err := json.Unmarshal(raw, &res); err != nil {
		fmt.Println(string(raw))
		return
	}
	fmt.Printf("ops=%d failed=%d dur_ms=%.1f throughput=%.0f ops/s p50=%.3fms p90=%.3fms p99=%.3fms max=%.3fms fsm_applied=%d\n",
		res.Ops, res.Failed, float64(res.DurationNs)/1e6, res.Throughput,
		float64(res.P50Ns)/1e6, float64(res.P90Ns)/1e6, float64(res.P99Ns)/1e6, float64(res.MaxNs)/1e6, res.Applied)
}
