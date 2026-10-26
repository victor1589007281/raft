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

// getState 拉取某个 vraftd 的 /state 并解析; 任何一步失败(连不上、body 不是 JSON)
// 都返回 nil, 让调用方把它当作"这个地址目前不可用"而不是致命错误。
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

// main 轮询所有目标地址直到发现 leader, 再把 /bench 请求 POST 给它并打印结果。
//
// 算法解读：
//
//	原理：集群里谁是 leader 只能问出来, 所以先做一段"发现"——每轮遍历全部地址,
//	      谁自报 Role=="Leader" 就选谁; 找到即跳出, 一轮都没有就 sleep 1s 再来。
//	      发现之后**只发一次** /bench, 因为基准本身跑在 vraftd 进程内, 驱动只负责
//	      挑目标与展示数字。
//	关键不变量：这里的耗时几乎全在 vraftd 侧, 驱动只做一次 HTTP 往返, 所以打印的
//	      吞吐/延迟就是被观测节点自己测出来的值, 不含客户端网络开销。也正因为如此,
//	      驱动不需要预热或多次取平均——重复运行要手动发起。
//	代价与边界：180s 是"等一个集群选完主"的上限, 超时就退出而不是继续等;
//	      探活失败与"不是 leader"被同样对待(都只是换个地址试), 所以集群全挂时
//	      表现为 180s 后报 "no leader found", 而不是立刻报连接错误。
//	设计权衡：不做负载生成、不做统计聚合, 把这两件事都留在 vraftd 内(那里才有
//	      真正的定时器与写路径); 代价是这个驱动只能测 vraftd, 不能测任意 raft 服务。
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
	// 180s 是"冷启动整个集群并等它选完主"的上限; 1s 轮询与 joinCluster 的节奏
	// 一致, 也远小于它, 不会让驱动比集群自己还慢。
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
		// 200 但结构对不上(例如服务端换了字段)时把原始 body 打出来而不是
		// 报错退出, 这样至少能看到对方到底回了什么。
		fmt.Println(string(raw))
		return
	}
	fmt.Printf("ops=%d failed=%d dur_ms=%.1f throughput=%.0f ops/s p50=%.3fms p90=%.3fms p99=%.3fms max=%.3fms fsm_applied=%d\n",
		res.Ops, res.Failed, float64(res.DurationNs)/1e6, res.Throughput,
		float64(res.P50Ns)/1e6, float64(res.P90Ns)/1e6, float64(res.P99Ns)/1e6, float64(res.MaxNs)/1e6, res.Applied)
}
