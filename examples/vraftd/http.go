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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// handleState reports the node's role and the cluster's progress.
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

// handleMetrics returns the go-metrics in-memory interval data as JSON, exposing
// the raft write-path latency distribution (raft.leader.logStore = the store
// write + fsync latency, raft.leader.asyncPersistBatchSize, ...).
//
// Units: the fork's metrics flow through go-metrics/compat into armon/go-metrics,
// whose default TimerGranularity is time.Millisecond. MeasureSince therefore
// divides elapsed.Nanoseconds() by 1e6, so every latency value below is in
// MILLISECONDS (min/mean/max), and AddSample sizes are plain counts.
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

// handleApply replicates a {"op":"set","k":...,"v":...} command through raft.
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
	f := n.raft.Apply(cmd, 10*time.Second)
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"index": f.Index(),
		"value": f.Response(),
	})
}

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

// handleRead returns the value for a key under one of the read-consistency
// tiers (12.2). The tier is selected with ?consistency=strict|lease|stale;
// "stale" (the historical behavior of a plain local FSM read) is the default so
// existing clients keep working. strict/lease go through VerifyReadIndex and
// wait for the FSM to apply at least the returned watermark before reading, so
// the served value is at least as fresh as the watermark.
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
		deadline := time.Now().Add(5 * time.Second)
		for n.raft.AppliedIndex() < watermark {
			if time.Now().After(deadline) {
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

// handleReadAll dumps the full replicated map.
func (n *Node) handleReadAll(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"applied": n.fsm.Applied(),
		"kv":      n.fsm.Dump(),
	})
}

// handleStoreInfo exposes the on-disk log store format and sparse-index
// state — the in-cluster evidence that the single-WAL v2 format (magic
// header, CRC, batch padding) is actually in effect (12.14).
func (n *Node) handleStoreInfo(w http.ResponseWriter, r *http.Request) {
	if n.store == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("no store"))
		return
	}
	writeJSON(w, http.StatusOK, n.store.Stats())
}

// handleLookupLSN resolves a redo LSN to its raft index through the sparse
// index (GET /lookup-lsn?lsn=N) — proves the index is built on writes and
// rebuilt on reload.
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
	idx, found := n.store.LookupByLSN(lsn)
	writeJSON(w, http.StatusOK, map[string]any{"lsn": lsn, "index": idx, "found": found})
}

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

// handleConfig exposes the latest raft configuration.
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
	writeJSON(w, http.StatusOK, map[string]any{"barrier": true, "index": f.(raft.IndexFuture).Index()})
}

// handleJoin is called by a new node to ask this (hopefully leader) node to add
// it as a voter. Only the leader can perform the AddVoter.
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

// handleRemove removes a voter (typically the node itself, on graceful leave).
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

// handlePromote promotes a learner to a full voter once it has caught up
// (12.5.3). The handler blocks until the catch-up window (default 60s, or
// ?timeout=Ns) elapses and the AddVoter entry commits.
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

// handleAdaptive is the runtime adapter for the adaptive-loop knobs (12.6).
// GET returns the current reloadable configuration (BatchWindow,
// MaxAppendEntries, ...). POST applies a partial update: only the fields
// present in the JSON body are changed, and only the vraft batching knobs are
// validated (heartbeat/election timers are intentionally ignored here — they
// are reloadable upstream but this control plane is reserved for the write-path
// adaptive loop).
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
		rc := n.raft.ReloadableConfig()
		if req.BatchWindowMs != nil {
			if *req.BatchWindowMs < 0 {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("batch_window_ms must be >= 0"))
				return
			}
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

// adaptiveState snapshots the reloadable config plus the node's batching knobs
// for reporting by /adaptive.
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

// handleKill shuts the raft instance down (fault injection for failover tests).
func (n *Node) handleKill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	go func() {
		if f := n.raft.Shutdown(); f.Error() != nil {
			log.Printf("[ERROR] shutdown: %v", f.Error())
		}
	}()
	writeJSON(w, http.StatusOK, map[string]any{"killing": n.id})
}

// handleBench runs an in-process load generator against this node.
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

// httpJoin asks addr (e.g. "10.0.0.5:9000") to add this node as a voter.
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
