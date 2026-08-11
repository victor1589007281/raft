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
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// Node wires the raft instance, the replicated KV FSM and the node's own
// identity into a small HTTP control plane.
type Node struct {
	id           string
	raft         *raft.Raft
	fsm          *KVFSM
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
	mux.HandleFunc("/kill", n.handleKill)
	mux.HandleFunc("/bench", n.handleBench)

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

// handleRead returns the local (possibly lagging) value for a key.
func (n *Node) handleRead(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("missing ?key= parameter"))
		return
	}
	val, ok := n.fsm.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"key": key, "found": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": val, "found": true})
}

// handleReadAll dumps the full replicated map.
func (n *Node) handleReadAll(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"applied": n.fsm.Applied(),
		"kv":      n.fsm.Dump(),
	})
}

// handleStats exposes the raw raft stats map.
func (n *Node) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, n.raft.Stats())
}

// handleConfig exposes the latest raft configuration.
func (n *Node) handleConfig(w http.ResponseWriter, r *http.Request) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"index":   f.Index(),
		"servers": f.Configuration().Servers,
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
