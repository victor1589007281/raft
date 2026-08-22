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
		useFdatasync = flag.Bool("use-fdatasync", false, "12.9: use fdatasync instead of fsync (skip inode mtime flush)")
		preallocSeg = flag.Bool("preallocate-segments", false, "12.9: preallocate next segment via fallocate")
		segSize     = flag.Int64("segment-size", 0, "12.9: segment size for preallocation (0=64MiB)")
		useFadvise  = flag.Bool("use-fadvise", false, "12.9: fadvise DONTNEED on truncated tail")
		useSFR      = flag.Bool("use-sync-file-range", false, "12.9: sync_file_range async writeback kick")
	ioring     = flag.Bool("ioring", false, "12.9: io_uring chain WRITE->FSYNC (probed, fallback to O_DSYNC)")
	ioringSQPoll = flag.Bool("ioring-sqpoll", false, "12.9: io_uring SQPOLL (requires -ioring)")
	directIO   = flag.Bool("direct-io", false, "12.9: O_DIRECT+IOPOLL tier (档位化，默认关)")
	singleWAL  = flag.Bool("single-wal", false, "12.7: unified WAL prototype (raft log == redo segment)")
	sparseIdx  = flag.Bool("sparse-index", false, "12.7: LSN->(index,off) sparse index (requires -single-wal)")
	fsmAsync   = flag.Bool("fsm-async-persist", false, "12.7: BatchingFSM.ApplyBatch pure bookkeeping (no Sync)")
	rpcRaw     = flag.Bool("rpc-raw-bytes", false, "12.8.2: raw-bytes GetLogRaw fast path")
	rpcWritev  = flag.Bool("rpc-writev", false, "12.8.3: writev/net.Buffers fan-out")
	rpcZCopy   = flag.Bool("rpc-zerocopy", false, "12.8.5: sendfile/splice zero-copy snapshot path")
	rpcBusyPoll = flag.Bool("rpc-busy-poll", false, "12.8.6: SO_BUSY_POLL on accepted conns")
	rpcIORing  = flag.Bool("rpc-ioring", false, "12.8.7: io_uring network probe")
	rpcLevel   = flag.Int("rpc-protocol-level", 0, "12.8.8: protocol tier 0=auto/off 1=baseline 2=dual 3=QUIC 4=RDMA (reserved)")
	tcpNoDelay = flag.Bool("tcp-nodelay", true, "12.8.4: TCP_NODELAY (default on)")
	tcpSndBuf  = flag.Int("tcp-sndbuf", 0, "12.8.4: SO_SNDBUF bytes (0=kernel default)")
	tcpUserTimeout = flag.Int("tcp-user-timeout", 0, "12.8.4: TCP_USER_TIMEOUT ms (0=off)")
	tcpBBR     = flag.Bool("tcp-bbr", false, "12.8.4: TCP_CONGESTION=bbr")
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
	logStore.useFdatasync = *useFdatasync
	logStore.preallocateSegments = *preallocSeg
	logStore.segmentSize = *segSize
	logStore.useFadvise = *useFadvise
	logStore.useSyncFileRange = *useSFR
	logStore.ioringEnabled = *ioring
	logStore.ioringSQPoll = *ioringSQPoll
	logStore.directIOEnabled = *directIO
	logStore.singleWALEnabled = *singleWAL
	logStore.singleWALSparseIndex = *sparseIdx
	stableStore, err := newFileStableStore(filepath.Join(*dataDir, "stable"))
	if err != nil {
		log.Fatalf("open stable store: %v", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(filepath.Join(*dataDir, "snapshots"), 2, os.Stderr)
	if err != nil {
		log.Fatalf("open snapshot store: %v", err)
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
	conf.UseFdatasync = *useFdatasync
	conf.PreallocateSegments = *preallocSeg
	conf.SegmentSize = *segSize
	conf.UseFadvise = *useFadvise
	conf.UseSyncFileRange = *useSFR
	conf.IORingEnabled = *ioring
	conf.IORingSQPoll = *ioringSQPoll
	conf.DirectIOEnabled = *directIO
	conf.SingleWALEnabled = *singleWAL
	conf.SingleWALSparseIndex = *sparseIdx
	conf.FsmAsyncPersist = *fsmAsync
	conf.RPCRawBytesEnabled = *rpcRaw
	conf.RPCWritevEnabled = *rpcWritev
	conf.RPCZeroCopyEnabled = *rpcZCopy
	conf.RPCBusyPollEnabled = *rpcBusyPoll
	conf.RPCIORingEnabled = *rpcIORing
	conf.RPCProtocolLevel = *rpcLevel
	if *tcpNoDelay { v:=true; conf.RPCTCPConfig.TCPNoDelay=&v } else { v:=false; conf.RPCTCPConfig.TCPNoDelay=&v }
	if *tcpSndBuf>0 { v:=*tcpSndBuf; conf.RPCTCPConfig.SndBuf=&v }
	if *tcpUserTimeout>0 { v:=*tcpUserTimeout; conf.RPCTCPConfig.UserTimeout=&v }
	if *tcpBBR { v:=true; conf.RPCTCPConfig.BBR=&v }

	var trans raft.Transport
	needNTC := *rpcRaw || *rpcWritev || *rpcZCopy || *rpcBusyPoll || *rpcIORing || *rpcLevel!=0 || *tcpSndBuf>0 || *tcpUserTimeout>0 || *tcpBBR
	if needNTC {
		ntc := &raft.NetworkTransportConfig{ MaxPool: 4, Timeout: 10*time.Second, RPCRawBytesEnabled: *rpcRaw, RPCWritevEnabled: *rpcWritev, RPCZeroCopyEnabled: *rpcZCopy, RPCBusyPollEnabled: *rpcBusyPoll, RPCIORingEnabled: *rpcIORing, RPCProtocolLevel: *rpcLevel, RPCTCPConfig: raft.TransportRPCTCPConfig{ TCPNoDelay: conf.RPCTCPConfig.TCPNoDelay, KeepAlive: conf.RPCTCPConfig.KeepAlive, SndBuf: conf.RPCTCPConfig.SndBuf, UserTimeout: conf.RPCTCPConfig.UserTimeout, BBR: conf.RPCTCPConfig.BBR } }
		if tr, err2 := raft.NewTCPTransportWithConfig(*raftAddr, adv, ntc); err2 != nil { log.Fatalf("open raft transport: %v", err2) } else { trans = tr }
	} else {
		if tr, err2 := raft.NewTCPTransport(*raftAddr, adv, 4, 10*time.Second, os.Stderr); err2 != nil { log.Fatalf("open raft transport: %v", err2) } else { trans = tr }
	}

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
