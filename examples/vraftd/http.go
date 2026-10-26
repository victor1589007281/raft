// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft/filestore"
)

// Node wires the raft instance, the replicated KV FSM and the node's own
// identity into a small HTTP control plane.
type Node struct {
	id           string
	raft         *raft.Raft
	fsm          *KVFSM
	store        *filestore.Store
	advertise    string
	httpAddr     string
	batchWindow  time.Duration
	asyncPersist bool
}

// httpServer 装配控制面路由表并返回 http.Server, 含两个由环境变量打开的诊断钩子。
// httpServer builds the control-plane mux.
func (n *Node) httpServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/state", n.handleState)
	mux.HandleFunc("/apply", n.handleApply)
	mux.HandleFunc("/del", n.handleDel)
	mux.HandleFunc("/read", n.handleRead)
	mux.HandleFunc("/readall", n.handleReadAll)
	mux.HandleFunc("/stats", n.handleStats)
	mux.HandleFunc("/config", n.handleConfig)
	mux.HandleFunc("/barrier", n.handleBarrier)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/join", n.handleJoin)
	mux.HandleFunc("/remove", n.handleRemove)
	mux.HandleFunc("/learner", n.handleLearner)
	mux.HandleFunc("/promote", n.handlePromote)
	mux.HandleFunc("/adaptive", n.handleAdaptive)
	mux.HandleFunc("/kill", n.handleKill)
	mux.HandleFunc("/bench", n.handleBench)
	mux.HandleFunc("/bench-read", n.handleBenchRead)
	// 12.14 single-WAL observability: on-disk format + sparse index probes.
	mux.HandleFunc("/storeinfo", n.handleStoreInfo)
	mux.HandleFunc("/lookup-lsn", n.handleLookupLSN)

	// Env-gated diagnostics for the async-persist hang investigation.
	// VRAFTD_DEBUG_NET=1 logs every accepted connection (ConnState) and every
	// request; VRAFTD_TICK=1 logs a liveness tick each second.
	var handler http.Handler = mux
	if os.Getenv("VRAFTD_DEBUG_NET") == "1" {
		log.Printf("[DEBUG] vraftd %s: http debug logging enabled", n.id)
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Printf("[DEBUG] vraftd %s: request %s %s", n.id, r.Method, r.URL.Path)
			mux.ServeHTTP(w, r)
			log.Printf("[DEBUG] vraftd %s: request done %s %s", n.id, r.Method, r.URL.Path)
		})
	}
	srv := &http.Server{Addr: n.httpAddr, Handler: handler}
	if os.Getenv("VRAFTD_DEBUG_NET") == "1" {
		srv.ConnState = func(c net.Conn, s http.ConnState) {
			log.Printf("[DEBUG] vraftd %s: connstate %s from %s", n.id, s.String(), c.RemoteAddr())
		}
	}
	return srv
}

// writeJSON 先写 Content-Type 与状态码, 再把 v 按 JSON 编码进响应体。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// 注意: WriteHeader 已经发出, 这里再出错也无法改状态码了, 所以刻意丢弃;
	// 客户端会看到一个截断的 JSON 体而不是 500。
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr 把错误包成 {"error":"..."} 并以给定状态码返回。
func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// handleState 汇报本节点的角色、任期与集群进度, 同时回显启动时定下的批量旋钮。
// handleState reports the node's role and the cluster's progress.
//
// 注意: batch_window_ns / async_persist 是**启动参数的回显**, 不是当前生效值——
// /adaptive 热更 BatchWindow 之后这里不会跟着变, 要读当前值得看 /adaptive。
func (n *Node) handleState(w http.ResponseWriter, r *http.Request) {
	leaderAddr, leaderID := n.raft.LeaderWithID()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":              n.id,
		"role":            n.raft.State().String(),
		"term":            n.raft.CurrentTerm(),
		"leader_id":       string(leaderID),
		"leader_addr":     string(leaderAddr),
		"commit_index":    n.raft.CommitIndex(),
		"applied_index":   n.raft.AppliedIndex(),
		"last_index":      n.raft.LastIndex(),
		"batch_window_ns": n.batchWindow.Nanoseconds(),
		"async_persist":   n.asyncPersist,
	})
}

// handleMetrics 把 go-metrics 的所有留存区间聚合成一次运行的总量并以 JSON 返回,
// 数值单位是毫秒。
// handleMetrics returns the go-metrics in-memory interval data as JSON, exposing
// the raft write-path latency distribution (raft.leader.logStore = the store
// write + fsync latency, raft.leader.asyncPersistBatchSize, ...).
//
// Units: the fork's metrics flow through go-metrics/compat into armon/go-metrics,
// whose default TimerGranularity is time.Millisecond. MeasureSince therefore
// divides elapsed.Nanoseconds() by 1e6, so every latency value below is in
// MILLISECONDS (min/mean/max), and AddSample sizes are plain counts.
//
// 算法解读：
//
//	原理：inmemSink 按 10s 一个区间保留 5 分钟样本, 而一次基准往往跨多个区间,
//	      所以这里必须**跨区间归并**: count 求和、sum 求和, min 取最小、max 取最大,
//	      最后用 sum/n 现算 mean。逐区间读数只会得到某个 10s 窗口的切面, 无法刻画
//	      整段写路径。
//	关键不变量：min/max 不能对零值取小/取大——一个区间里 Count==0 的样本 Min/Max
//	      都是 0, 直接参与比较会把 min 拉成 0。代码用 nm==0(本键此前还没收到过非空
//	      样本)作为"首次赋值"的判据, 这是全段唯一防止 0 值污染的地方。
//	代价与边界：n 只累加 Count>0 的样本, 与 count 的口径不同(count 把空样本也算
//	      了), 所以 mean 的分子分母是配套的, 而 count 字段单独看没有意义。区间数
//	      有上限(5min/10s=30), 所以这个聚合是 O(区间数 × 指标数), 不会随运行时长
//	      增长。
//	设计权衡：不做直方图、不给分位, 是因为 go-metrics 的 Sample 只有 min/max/sum;
//	      要分位得换 sink(或自己攒样本), 演示程序选了"不引入新依赖"。
func (n *Node) handleMetrics(w http.ResponseWriter, r *http.Request) {
	intervals := inmemSink.Data()
	// Aggregate across every retained interval: a benchmark run spans several
	// 10s intervals, and only the whole-run total characterizes the write path.
	byKey := map[string]map[string]any{}
	var lastEnd time.Time
	for _, iv := range intervals {
		iv.RLock()
		for name, s := range iv.Samples {
			a := byKey[name]
			if a == nil {
				// 首见该指标时用本样本的 Min/Max 起头; 若该样本是空的(Count==0),
				// 后面的 nm==0 分支会在遇到第一个非空样本时把它们覆盖掉。
				a = map[string]any{"count": int64(0), "min": s.Min, "max": s.Max, "sum": float64(0), "n": 0}
				byKey[name] = a
			}
			a["count"] = a["count"].(int64) + int64(s.Count)
			if s.Count > 0 {
				nm := a["n"].(int)
				a["n"] = nm + s.Count
				a["sum"] = a["sum"].(float64) + s.Sum
				if s.Min < a["min"].(float64) || nm == 0 {
					a["min"] = s.Min
				}
				if s.Max > a["max"].(float64) || nm == 0 {
					a["max"] = s.Max
				}
			}
		}
		iv.RUnlock()
		if iv.Interval.After(lastEnd) {
			lastEnd = iv.Interval
		}
	}
	out := map[string]any{}
	if len(byKey) > 0 {
		for _, a := range byKey {
			if a["n"].(int) > 0 {
				a["mean"] = a["sum"].(float64) / float64(a["n"].(int))
			}
			delete(a, "sum")
			delete(a, "n")
		}
		out["interval_end"] = lastEnd.UTC().Format(time.RFC3339Nano)
		out["samples"] = byKey
	}
	writeJSON(w, http.StatusOK, out)
}

// handleApply 把 {"k","v"} 包成 set 命令走 raft 复制, 成功时返回提交索引与 FSM 回执。
// handleApply replicates a {"op":"set","k":...,"v":...} command through raft.
//
// 注意: f.Error() 只在 raft 层面失败(未当选/超时/已关闭)时非 nil; FSM 自己返回的
// error 会进 f.Response(), 被 JSON 序列化成 {} 后仍然以 200 返回。
func (n *Node) handleApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req struct {
		K string `json:"k"`
		V string `json:"v"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cmd, err := json.Marshal(kvCmd{Op: "set", K: req.K, V: req.V})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// 10s 是"选举+复制"都能容纳的量级(默认 election timeout 1s), 超过它基本
	// 意味着本节点已不是 leader 或集群失去多数派。
	f := n.raft.Apply(cmd, 10*time.Second)
	if err := f.Error(); err != nil {
		// 503 而不是 500: 失败原因是"当前没有可用的 leader", 属于暂时性不可用。
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"index": f.Index(),
		"value": f.Response(),
	})
}

// handleDel 把 {"k"} 包成 del 命令走 raft 复制, 成功时返回提交索引。
// handleDel replicates a delete command.
func (n *Node) handleDel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req struct {
		K string `json:"k"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cmd, err := json.Marshal(kvCmd{Op: "del", K: req.K})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	f := n.raft.Apply(cmd, 10*time.Second)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"index": f.Index()})
}

// handleRead 按 ?consistency= 指定的档位读一个键; strict/lease 会等到 FSM 追平
// 该档位给出的水位之后才返回, 因此拿到的值不会比档位承诺的更旧。
// handleRead returns the value for a key under one of the read-consistency
// tiers (12.2). The tier is selected with ?consistency=strict|lease|stale;
// "stale" (the historical behavior of a plain local FSM read) is the default so
// existing clients keep working. strict/lease go through VerifyReadIndex and
// wait for the FSM to apply at least the returned watermark before reading, so
// the served value is at least as fresh as the watermark.
//
// 算法解读：
//
//	原理：VerifyReadIndex 只回答"读到哪个索引为止是安全的", 并不保证本地 FSM
//	      已经应用到那里。所以 strict/lease 走两段式: 先拿水位, 再自旋等
//	      r.AppliedIndex() 越过水位, 最后才读 FSM。stale 跳过第二段, 直接读本地
//	      状态, 这就是它最便宜也最可能读到旧值的原因。
//	关键不变量：等待用的是 **raft 的 applied index** 而不是 KVFSM 的命令计数——
//	      noop 与 config 条目会推进前者却永远不进 FSM, 用后者做判据会永久自旋。
//	      同理, 水位是 commit index, 单调不减, 所以自旋必然终止(除非节点卡住)。
//	代价与边界：strict 每次读都要一次 quorum 往返(VerifyLeader), 吞吐随集群规模
//	      下降; lease 只在租约有效时免掉这次往返, 租约一失效就静默回落成 strict,
//	      于是同一条 /read?consistency=lease 的延迟会在租约到期边界上出现双峰。
//	设计权衡：默认档位选 stale 而不是 strict, 是为了保持"普通本地读"的历史语义,
//	      让既有客户端升级后行为不变; 线性化的代价留给调用方显式选择。
func (n *Node) handleRead(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("missing ?key= parameter"))
		return
	}
	var consistency raft.ReadConsistency
	switch q := r.URL.Query().Get("consistency"); q {
	case "", "stale":
		consistency = raft.ReadStale
	case "lease":
		consistency = raft.ReadLease
	case "strict":
		consistency = raft.ReadStrict
	default:
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown consistency %q (want strict|lease|stale)", q))
		return
	}

	watermark, err := n.raft.VerifyReadIndex(consistency)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	// For strict/lease, stall until the FSM has applied the watermark so the
	// returned value cannot be older than the consistency tier promised. The
	// application progress is raft's own applied index (r.AppliedIndex), not the
	// KVFSM command counter: noop/config entries advance raft's applied index
	// without ever reaching the FSM, so the counter always lags the log index.
	if consistency != raft.ReadStale {
		// 5s 是"正常追平远快于此、真卡住时又不想让请求挂死"的折中; 1ms 轮询
		// 是刻意的粗粒度——读路径要的是低开销, 不差这一毫秒。
		deadline := time.Now().Add(5 * time.Second)
		for n.raft.AppliedIndex() < watermark {
			if time.Now().After(deadline) {
				// 504 而不是 503: 请求本身没问题, 是本地追赶超时。
				writeErr(w, http.StatusGatewayTimeout,
					fmt.Errorf("raft applied %d still behind watermark %d", n.raft.AppliedIndex(), watermark))
				return
			}
			time.Sleep(time.Millisecond)
		}
	}

	val, ok := n.fsm.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"key": key, "found": false, "consistency": consistency.String(),
			"watermark": watermark,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key": key, "value": val, "found": true,
		"consistency": consistency.String(), "watermark": watermark,
	})
}

// handleReadAll 导出整张复制映射与 FSM 的命令计数, 仅供调试, 状态大时会返回巨量 JSON。
// handleReadAll dumps the full replicated map.
func (n *Node) handleReadAll(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"applied": n.fsm.Applied(),
		"kv":      n.fsm.Dump(),
	})
}

// handleStoreInfo 返回日志存储的磁盘格式与稀疏索引状态, 是"单 WAL v2 格式确实
// 生效"的集群内证据。
// handleStoreInfo exposes the on-disk log store format and sparse-index
// state — the in-cluster evidence that the single-WAL v2 format (magic
// header, CRC, batch padding) is actually in effect (12.14).
//
// 算法解读：
//
//	原理：这个 handler 本身不做事, 只是把 filestore.Store.Stats() 的只读快照
//	      透出去; 真正的机制在 Store 里——它在 s.mu.RLock 下一次性读齐 format、
//	      lsnIndex 长度、首末索引、段尾偏移与各类计数器, 所以返回的是一致切面
//	      而不是若干次独立读的拼接。
//	关键不变量：Stats() 取的是读锁, 因此它可以与写路径并发调用而不阻塞写入;
//	      也正因为如此, 它读到的 lastIndex 与 endOffset 之间可能存在"已经写入
//	      但尚未登记"的瞬时错位, 观测时应把它当近似值。
//	代价与边界：len(s.lsnIndex) 是 O(1) 的; 整个调用不触碰磁盘, 因此 format 字段
//	      反映的是**打开时判定的格式**, 不会因为运行期换文件而改变。n.store 为
//	      nil 时(理论上只在未启用文件存储的构造路径上)返回 503。
//	设计权衡：直接暴露 Stats 结构体而不是在 handler 里重新组 JSON, 是为了让
//	      "观测面"与"库内定义"只有一个来源; 代价是 filestore.Stats 的字段变动
//	      会直接改变这个 HTTP 接口的形状。
func (n *Node) handleStoreInfo(w http.ResponseWriter, r *http.Request) {
	if n.store == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("no store"))
		return
	}
	writeJSON(w, http.StatusOK, n.store.Stats())
}

// handleLookupLSN 用稀疏索引把 redo LSN 解析成 raft 索引 (GET /lookup-lsn?lsn=N),
// 用来证明索引在写入时建立、在重载时重建。
// handleLookupLSN resolves a redo LSN to its raft index through the sparse
// index (GET /lookup-lsn?lsn=N) — proves the index is built on writes and
// rebuilt on reload.
//
// 算法解读：
//
//	原理：单 WAL 下 raft 日志条目与 redo 段共用一份文件, 每个条目落盘时携带
//	      自己的 LSN; 稀疏索引只记"某些 LSN → (raft index, 段内偏移)"的抽样点,
//	      所以查询是 O(1) 的 map 命中, 而不是扫描整个段。
//	关键不变量：稀疏意味着**未命中不等于不存在**——found=false 只说明这个 LSN
//	      没有被抽样进索引, 不能据此判断数据缺失(见下面 found 字段的语义)。
//	      另一个不变量是索引与数据同源: 索引由写入路径顺带建立, 重载时从段文件
//	      重建, 所以两者不会各自漂移。
//	代价与边界：内存占用与索引条目数成正比(而不是与 LSN 总数成正比), 这正是
//	      "稀疏"的取舍; 代价是查询方需要自己处理未命中。lsn 参数用 ParseUint
//	      解析, 空串或非数字一律 400。
//	设计权衡：把解析结果连同 found 一起返回(而不是未命中报 404), 是因为这个
//	      接口的用途是**观测索引状态**, 未命中本身就是有价值的信息。
func (n *Node) handleLookupLSN(w http.ResponseWriter, r *http.Request) {
	if n.store == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("no store"))
		return
	}
	lsn, err := strconv.ParseUint(r.URL.Query().Get("lsn"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad lsn: %v", err))
		return
	}
	// found=false 时 idx 是零值, 调用方必须看 found 而不是看 index==0;
	// raft 索引从 1 开始, 但这里刻意不给"0 即未命中"的隐含约定。
	idx, found := n.store.LookupByLSN(lsn)
	writeJSON(w, http.StatusOK, map[string]any{"lsn": lsn, "index": idx, "found": found})
}

// handleStats 原样透出 raft.Stats() 的统计 map, 不做任何字段筛选或单位换算。
// handleStats exposes the raw raft stats map.
func (n *Node) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, n.raft.Stats())
}

// configServer is the JSON projection of a raft.Server; the field names are
// lowercased so the payload matches the rest of the control API.
type configServer struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage int    `json:"suffrage"`
}

// handleConfig 返回最新一次 raft 配置及其索引, 成员字段名与其余控制接口保持一致。
// handleConfig exposes the latest raft configuration.
//
// 注意: GetConfiguration 给的是 **latest** 而不是已提交的那一份, 正在进行的成员
// 变更会立刻出现在这里, 所以它不能当作"已生效"的判据。
func (n *Node) handleConfig(w http.ResponseWriter, r *http.Request) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	cfg := f.Configuration()
	servers := make([]configServer, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		servers = append(servers, configServer{
			ID:       string(s.ID),
			Address:  string(s.Address),
			Suffrage: int(s.Suffrage),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"index":   f.Index(),
		"servers": servers,
	})
}

// handleBarrier 阻塞到 FSM 消费完此前所有已应用命令, 常用于基准前把状态对齐。
// handleBarrier blocks until every applied command has been consumed by the FSM.
func (n *Node) handleBarrier(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	f := n.raft.Barrier(10 * time.Second)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	// 这里是对具体类型的断言而不是带 ok 的形式: Barrier 返回的 future 实际就是
	// indexFuture, 若上游改了返回类型, 这里会直接 panic 而不是返回 500。
	writeJSON(w, http.StatusOK, map[string]any{"barrier": true, "index": f.(raft.IndexFuture).Index()})
}

// handleJoin 由新节点调用, 请求本节点把它加为 voter; 只有 leader 才能真正执行 AddVoter。
// handleJoin is called by a new node to ask this (hopefully leader) node to add
// it as a voter. Only the leader can perform the AddVoter.
//
// 注意: 第三个参数(prevIndex)传 0 表示"不要求新成员先追平任何索引", 新 voter 会
// 立刻计入多数派; 这正是 joinCluster 需要反复重试的原因——非 leader 会返回错误。
func (n *Node) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.ID == "" || req.Address == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("id and address are required"))
		return
	}
	f := n.raft.AddVoter(raft.ServerID(req.ID), raft.ServerAddress(req.Address), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"joined":  true,
		"id":      req.ID,
		"address": req.Address,
		"index":   f.Index(),
	})
}

// handleRemove 把一个 voter 移出集群; 优雅退出时通常由节点对自己发起。
// handleRemove removes a voter (typically the node itself, on graceful leave).
//
// 注意: 这里不校验 req.ID 是否存在, 也不校验它是不是本节点——移除一个不在配置里
// 的 id 同样会成功(raft 只保证配置变更被复制并提交)。
func (n *Node) handleRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	f := n.raft.RemoveServer(raft.ServerID(req.ID), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": req.ID, "index": f.Index()})
}

// handleLearner 把节点以非投票 learner 身份加入: 它照收日志与快照, 但不计入提交
// quorum, 因此扩容不会削弱可用性。
// handleLearner adds a node as a non-voting learner (12.5.3). Like /join but
// for AddLearner: the member receives log entries / snapshots and is excluded
// from the commit quorum, so scaling out never degrades availability. Promote
// it with /promote once it has caught up.
func (n *Node) handleLearner(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.ID == "" || req.Address == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("id and address are required"))
		return
	}
	f := n.raft.AddLearner(raft.ServerID(req.ID), raft.ServerAddress(req.Address), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"learner": true, "id": req.ID, "address": req.Address, "index": f.Index(),
	})
}

// handlePromote 在 learner 追平后把它晋升为正式 voter, 默认等待 60s 的追平窗口。
// handlePromote promotes a learner to a full voter once it has caught up
// (12.5.3). The handler blocks until the catch-up window (default 60s, or
// ?timeout=Ns) elapses and the AddVoter entry commits.
//
// 注意: ?timeout= 解析失败时**静默**退回 60s 默认值(不报 400), 写错的单位会变成
// 一次很长的阻塞; 另外这个 handler 会一直阻塞到窗口结束, 属同步长请求。
func (n *Node) handlePromote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.ID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("id is required"))
		return
	}
	// 60s 覆盖"learner 落后一整个快照再追平"的常见量级; 解析失败按原样保留 60s。
	timeout := 60 * time.Second
	if ts := r.URL.Query().Get("timeout"); ts != "" {
		if d, err := time.ParseDuration(ts); err == nil {
			timeout = d
		}
	}
	f := n.raft.PromoteToVoter(raft.ServerID(req.ID), timeout)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"promoted": req.ID, "index": f.Index(),
	})
}

// handleAdaptive 是自适应旋钮的运行时入口: GET 读回可热更配置, POST 做部分更新
// (只改请求体里出现的字段)。
// handleAdaptive is the runtime adapter for the adaptive-loop knobs (12.6).
// GET returns the current reloadable configuration (BatchWindow,
// MaxAppendEntries, ...). POST applies a partial update: only the fields
// present in the JSON body are changed, and only the vraft batching knobs are
// validated (heartbeat/election timers are intentionally ignored here — they
// are reloadable upstream but this control plane is reserved for the write-path
// adaptive loop).
//
// 算法解读：
//
//	原理：用指针字段(*int)区分"没传"和"传了 0"——只有 JSON 体里真正出现的字段
//	      才会被写进 rc, 其余保持 ReloadableConfig() 读回来的当前值, 这就是
//	      "部分更新"的实现方式。改完把整个 rc 交给 ReloadConfig, 由它做校验、
//	      合并与原子替换。
//	关键不变量：BatchWindow 与 MaxAppendEntries 在 ReloadableConfig.apply 里是
//	      **非零才应用**的。于是 batch_window_ms=0 会被静默丢弃、保留旧窗口——
//	      这里放行了 0(校验是 < 0 才报错), 但 0 并**不能**关掉组提交窗口, 这是
//	      一条从接口看不出来的语义缺口。
//	代价与边界：这是"读-改-写"两步, ReloadableConfig 的文档明确说它不保证与
//	      并发写者互斥, 所以两个并发的 POST 可能互相覆盖对方改的另一个字段。
//	      单字段更新没问题, 多字段并发更新有丢失更新风险。
//	设计权衡：故意不在这里校验 heartbeat/election 等定时器, 把它们排除在控制面
//	      之外, 以免自适应循环误改选举参数导致集群失稳; 代价是这些字段仍然会被
//	      ReloadConfig 用 rc 里的旧值原样写回(等于没变)。
func (n *Node) handleAdaptive(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, n.adaptiveState())
	case http.MethodPost:
		var req struct {
			BatchWindowMs    *int `json:"batch_window_ms"`
			MaxAppendEntries *int `json:"max_append_entries"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		// 读-改-写: rc 是当前值的完整快照, 下面只覆盖请求里出现的字段。
		rc := n.raft.ReloadableConfig()
		if req.BatchWindowMs != nil {
			if *req.BatchWindowMs < 0 {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("batch_window_ms must be >= 0"))
				return
			}
			// 注意: 传 0 能通过校验, 但 apply() 只在 >0 时才写入配置, 所以
			// "把窗口关掉"这个意图会被静默忽略, 旧窗口继续生效。
			rc.BatchWindow = time.Duration(*req.BatchWindowMs) * time.Millisecond
		}
		if req.MaxAppendEntries != nil {
			rc.MaxAppendEntries = *req.MaxAppendEntries
		}
		if err := n.raft.ReloadConfig(rc); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, n.adaptiveState())
	default:
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("GET or POST required"))
	}
}

// adaptiveState 快照可热更配置与本节点批量旋钮, 供 /adaptive 的 GET/POST 回显。
// adaptiveState snapshots the reloadable config plus the node's batching knobs
// for reporting by /adaptive.
//
// 注意: 这里读的是 ReloadableConfig(), 即**当前生效值**, 与 /state 回显的启动参数
// 是两回事; 热更 BatchWindow 之后只有这里会变。
func (n *Node) adaptiveState() map[string]any {
	rc := n.raft.ReloadableConfig()
	return map[string]any{
		"batch_window_ms":      rc.BatchWindow.Milliseconds(),
		"max_append_entries":   rc.MaxAppendEntries,
		"heartbeat_timeout_ms": rc.HeartbeatTimeout.Milliseconds(),
		"election_timeout_ms":  rc.ElectionTimeout.Milliseconds(),
		"trailing_logs":        rc.TrailingLogs,
		"snapshot_threshold":   rc.SnapshotThreshold,
		"snapshot_interval_ms": rc.SnapshotInterval.Milliseconds(),
	}
}

// handleKill 异步关停本节点的 raft 实例, 用于故障注入与主备切换测试。
// handleKill shuts the raft instance down (fault injection for failover tests).
//
// 注意: 关的是 raft 而不是进程——HTTP 控制面会继续活着(这也是刻意设计: 让测试
// 仍能查询一个"已死"节点的状态), 只有重启进程才会真正退出。
func (n *Node) handleKill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	// 必须放到 goroutine 里: Shutdown 会等内部各循环退出, 在 handler 里同步等
	// 会把响应压在后面, 调用方反而看不到 "killing" 回执。
	go func() {
		if f := n.raft.Shutdown(); f.Error() != nil {
			log.Printf("[ERROR] shutdown: %v", f.Error())
		}
	}()
	writeJSON(w, http.StatusOK, map[string]any{"killing": n.id})
}

// handleBench 在本进程内对 leader 跑一次写基准, 请求体即 benchReq。
// handleBench runs an in-process load generator against this node.
//
// 注意: 基准跑在 vraftd 进程内(定时器与写路径都在这里), 所以测得的吞吐/延迟就是
// 真实的 raft 写路径数字; 但它是**同步**的, 一次请求会占住这个 handler 直到跑完。
func (n *Node) handleBench(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req benchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := runBench(n.raft, n.fsm, req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleBenchRead 在本进程内跑一次读一致性档位基准 (12.2), 请求体即 readBenchReq。
// handleBenchRead runs an in-process read-tier benchmark against this node
// (12.2): POST {"n":..., "c":..., "consistency":"strict|lease|stale"}.
func (n *Node) handleBenchRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	var req readBenchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := runReadBench(n.raft, n.fsm, req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// httpJoin 请求 addr (如 "10.0.0.5:9000") 把本节点加为 voter。
// httpJoin asks addr (e.g. "10.0.0.5:9000") to add this node as a voter.
//
// 注意: body 里带的 address 是 n.advertise, 即 **raft 通信地址**, 不是 HTTP 地址;
// 两者在部署里通常不同端口, 传错会让新成员加入后互相连不上。
func (n *Node) httpJoin(addr string) error {
	body, err := json.Marshal(map[string]string{"id": n.id, "address": n.advertise})
	if err != nil {
		return err
	}
	u := "http://" + addr + "/join"
	resp, err := http.Post(u, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("join %s: %s: %s", u, resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}
