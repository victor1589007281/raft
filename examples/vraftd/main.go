// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

// Command vraftd is the vRaft demo application: a small replicated KV store on
// top of the vraft fork of hashicorp/raft. It exercises the vraft write-path
// features (BatchWindow group-commit, async leader persist) in a real
// file-backed process and serves as the container payload for the kind k8s
// deployment.
//
// A single node starts a one-node cluster with -bootstrap; additional nodes
// start empty and join through the leader's HTTP /join endpoint (either by
// passing -join addresses on the command line or by POSTing to /join).
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	armonmetrics "github.com/armon/go-metrics"
	"github.com/hashicorp/raft"
)

// inmemSink holds the armon/go-metrics in-memory interval buffer used by the
// raft fork's write-path telemetry (raft.leader.logStore,
// raft.leader.asyncPersistBatchSize, etc.) and is served read-only via /metrics.
// The fork emits through the compat package, which (absent the
// "hashicorpmetrics" build tag) proxies to armon/go-metrics.
var inmemSink = armonmetrics.NewInmemSink(10*time.Second, 5*time.Minute)

func main() {
	var (
		id          = flag.String("id", "", "unique server ID")
		httpAddr    = flag.String("http", "127.0.0.1:9000", "control API listen address")
		raftAddr    = flag.String("raft", "127.0.0.1:9001", "raft bind address")
		advertise   = flag.String("raft-advertise", "", "raft advertise address (defaults to the bind address)")
		dataDir     = flag.String("data-dir", "vraft-data", "data directory")
		bootstrap   = flag.Bool("bootstrap", false, "bootstrap a new single-node cluster")
		joinAddrs   = flag.String("join", "", "comma-separated HTTP addresses of cluster members to join")
		batchWin    = flag.Duration("batch-window", 0, "vraft: leader group-commit window (0 disables; e.g. 2ms)")
		asyncPers   = flag.Bool("async-persist", false, "vraft: leader persists asynchronously, overlapping its fsync with replication")
		snapThresh  = flag.Uint64("snapshot-threshold", 8192, "logs outstanding before a snapshot is triggered")
		noSnap      = flag.Bool("no-snapshot", false, "disable periodic snapshots (clean benchmark runs)")
		maxAppend   = flag.Int("max-append-entries", 64, "max AppendEntries in a single batch")
		leaderLease = flag.Duration("leader-lease", 0, "leader lease timeout (0 uses raft default)")
	)
	flag.Parse()

	if *id == "" {
		log.Fatal("missing required flag -id")
	}

	// Sanity: bootstrap and join are mutually exclusive ways to enter a cluster.
	if *bootstrap && *joinAddrs != "" {
		log.Fatal("-bootstrap and -join are mutually exclusive")
	}

	// Resolve the raft advertise address. This may be a concrete IP or a
	// hostname:port pair (e.g. a k8s StatefulSet DNS name). NewTCPTransport
	// rejects 0.0.0.0 as unadvertisable; a hostname is stored verbatim in the
	// raft configuration so peer addresses survive pod re-creation.
	advertiseAddr := *raftAddr
	if *advertise != "" {
		advertiseAddr = *advertise
	}
	adv, err := makeAdvertiseAddr(advertiseAddr)
	if err != nil {
		log.Fatalf("invalid raft advertise address %q: %v", advertiseAddr, err)
	}

	// File-backed stores.
	logPath := filepath.Join(*dataDir, "logs", "raft.log")
	hadData := fileExists(logPath)
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}
	logStore, err := newFileLogStore(filepath.Join(*dataDir, "logs"))
	if err != nil {
		log.Fatalf("open log store: %v", err)
	}
	stableStore, err := newFileStableStore(filepath.Join(*dataDir, "stable"))
	if err != nil {
		log.Fatalf("open stable store: %v", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(filepath.Join(*dataDir, "snapshots"), 2, os.Stderr)
	if err != nil {
		log.Fatalf("open snapshot store: %v", err)
	}

	trans, err := raft.NewTCPTransport(*raftAddr, adv, 4, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("open raft transport: %v", err)
	}

	fsm := newKVFSM()

	// Route the fork's raft telemetry to the in-memory sink so the write-path
	// latency distribution is observable via /metrics.
	if _, err := armonmetrics.NewGlobal(armonmetrics.DefaultConfig("vraftd"), inmemSink); err != nil {
		log.Fatalf("init metrics sink: %v", err)
	}

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(*id)
	conf.MaxAppendEntries = *maxAppend
	if *noSnap {
		// Effectively disable log compaction for clean benchmark runs.
		conf.SnapshotThreshold = 1 << 32
		conf.SnapshotInterval = 24 * time.Hour
	} else {
		conf.SnapshotThreshold = *snapThresh
	}
	if *leaderLease > 0 {
		conf.LeaderLeaseTimeout = *leaderLease
	}
	// vraft write-path features.
	conf.BatchWindow = *batchWin
	conf.AsyncLeaderPersist = *asyncPers

	// A fresh node either bootstraps itself (first member) or joins an existing
	// cluster. A node with on-disk data just restarts and recovers its role.
	if !hadData {
		switch {
		case *bootstrap:
			configuration := raft.Configuration{
				Servers: []raft.Server{{
					ID:      raft.ServerID(*id),
					Address: raft.ServerAddress(advertiseAddr),
				}},
			}
			if err := raft.BootstrapCluster(conf, logStore, stableStore, snapStore, trans, configuration); err != nil {
				log.Fatalf("bootstrap cluster: %v", err)
			}
			log.Printf("[INFO] vraftd %s bootstrapped single-node cluster at %s", *id, advertiseAddr)
		case *joinAddrs != "":
			log.Printf("[INFO] vraftd %s starting, will join %s", *id, *joinAddrs)
		default:
			log.Fatal("fresh node requires -bootstrap (first member) or -join <candidates>")
		}
	} else {
		log.Printf("[INFO] vraftd %s restarting with existing data", *id)
	}

	r, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapStore, trans)
	if err != nil {
		log.Fatalf("NewRaft: %v", err)
	}

	node := &Node{
		id:           *id,
		raft:         r,
		fsm:          fsm,
		advertise:    advertiseAddr,
		httpAddr:     *httpAddr,
		batchWindow:  *batchWin,
		asyncPersist: *asyncPers,
	}
	srv := node.httpServer()

	// Join the cluster if requested. Do this in the background: the cluster may
	// still be electing, and /join retries until the leader accepts us.
	if !hadData && *joinAddrs != "" {
		go joinCluster(node, *joinAddrs)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("[INFO] vraftd %s control API listening on %s, raft on %s (advertise %s)",
		*id, *httpAddr, *raftAddr, advertiseAddr)

	if os.Getenv("VRAFTD_TICK") == "1" {
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for range t.C {
				log.Printf("[TICK] vraftd %s alive goroutines=%d", *id, runtime.NumGoroutine())
			}
		}()
	}

	// Wait for SIGINT/SIGTERM, then shut down raft cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		log.Printf("[INFO] vraftd %s shutting down", *id)
	case err := <-errCh:
		if err != nil {
			log.Printf("[ERROR] http server: %v", err)
		}
	}

	if f := r.Shutdown(); f.Error() != nil {
		log.Printf("[ERROR] raft shutdown: %v", f.Error())
	}
	if err := logStore.Close(); err != nil {
		log.Printf("[ERROR] closing log store: %v", err)
	}
}

// fileExists reports whether a path exists.
func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// makeAdvertiseAddr turns a "host:port" advertise string into the net.Addr to
// hand to NewTCPTransport. IP literals resolve to *net.TCPAddr; hostnames are
// wrapped in raft.NewHostnameAddr so raft stores the DNS name rather than a
// potentially-stale IP.
func makeAdvertiseAddr(addr string) (net.Addr, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(host) == nil {
		return raft.NewHostnameAddr(addr), nil
	}
	return net.ResolveTCPAddr("tcp", addr)
}

// joinCluster asks the given HTTP addresses (in order) to add this node as a
// voter. It retries until a leader acknowledges the join. Only the leader can
// AddVoter, so non-leader responses are ignored and we try the next address.
func joinCluster(node *Node, addrs string) {
	candidates := strings.Split(addrs, ",")
	for {
		for _, a := range candidates {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			err := node.httpJoin(a)
			if err == nil {
				log.Printf("[INFO] joined cluster via %s", a)
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
}
