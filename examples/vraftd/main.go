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
	"github.com/hashicorp/raft/filestore"
)

// inmemSink holds the armon/go-metrics in-memory interval buffer used by the
// raft fork's write-path telemetry (raft.leader.logStore,
// raft.leader.asyncPersistBatchSize, etc.) and is served read-only via /metrics.
// The fork emits through the compat package, which (absent the
// "hashicorpmetrics" build tag) proxies to armon/go-metrics.
var inmemSink = armonmetrics.NewInmemSink(10*time.Second, 5*time.Minute)

// main 解析全部开关、装配 filestore/快照/传输三件套, 按"是否有本地数据"选择
// 自举或加入集群, 然后启动控制面并等到信号后优雅退出。
//
// 算法解读：
//
//	原理：启动路径由两个事实决定——磁盘上有没有 raft.log(hadData), 以及命令行给的是
//	      -bootstrap 还是 -join。四象限里只有"无数据 + 有 bootstrap"会真正调用
//	      BootstrapCluster 写入创世配置; "无数据 + 有 join"只是记一条日志, 真正的
//	      入群由后台 goroutine 反复 POST /join 完成; "有数据"则两种情况都跳过,
//	      节点直接带着已落盘的配置重启并自己恢复角色。
//	关键不变量：BootstrapCluster 只能对**空白存储**调用一次, 所以 hadData 的判断
//	      必须在 NewRaft 之前、且以 raft.log 的存在为准; 顺序反了会把一个已有集群
//	      的成员重新自举成一个单节点新集群(数据仍在, 但配置被覆盖)。
//	      另一个不变量是 advertiseAddr 有两份表示: 交给传输层的是 net.Addr(主机名
//	      会被包成 HostnameAddr), 写进 bootstrap 配置的是**原始字符串**; 二者必须
//	      指向同一个地址, 否则成员互相找不到。
//	代价与边界：所有开关都是"启动时一次性"的, 只有 BatchWindow/MaxAppendEntries
//	      等少数几个能事后热更(见 /adaptive); 其余(如传输层配置)改完必须重启。
//	      任何一个开关解析失败都走 log.Fatalf 直接退出, 没有降级路径。
//	设计权衡：单进程同时承载 raft 与控制面 HTTP, 而不是拆两个二进制——演示程序
//	      优先"一眼看懂数据流"; 代价是控制面故障与 raft 故障无法独立重启。
func main() {
	var (
		id             = flag.String("id", "", "unique server ID")
		httpAddr       = flag.String("http", "127.0.0.1:9000", "control API listen address")
		raftAddr       = flag.String("raft", "127.0.0.1:9001", "raft bind address")
		advertise      = flag.String("raft-advertise", "", "raft advertise address (defaults to the bind address)")
		dataDir        = flag.String("data-dir", "vraft-data", "data directory")
		bootstrap      = flag.Bool("bootstrap", false, "bootstrap a new single-node cluster")
		joinAddrs      = flag.String("join", "", "comma-separated HTTP addresses of cluster members to join")
		batchWin       = flag.Duration("batch-window", 0, "vraft: leader group-commit window (0 disables; e.g. 2ms)")
		asyncPers      = flag.Bool("async-persist", false, "vraft: leader persists asynchronously, overlapping its fsync with replication")
		useFdatasync   = flag.Bool("use-fdatasync", false, "12.9: use fdatasync instead of fsync (skip inode mtime flush)")
		preallocSeg    = flag.Bool("preallocate-segments", false, "12.9: preallocate next segment via fallocate")
		segSize        = flag.Int64("segment-size", 0, "12.9: segment size for preallocation (0=64MiB)")
		useFadvise     = flag.Bool("use-fadvise", false, "12.9: fadvise DONTNEED on truncated tail")
		useSFR         = flag.Bool("use-sync-file-range", false, "12.9: sync_file_range async writeback kick")
		ioring         = flag.Bool("ioring", false, "12.9: io_uring chain WRITE->FSYNC (probed, fallback to O_DSYNC)")
		ioringSQPoll   = flag.Bool("ioring-sqpoll", false, "12.9: io_uring SQPOLL (requires -ioring)")
		directIO       = flag.Bool("direct-io", false, "12.9: O_DIRECT+IOPOLL tier (档位化，默认关)")
		singleWAL      = flag.Bool("single-wal", false, "12.7/12.14: unified WAL v2 (magic+CRC+batch-pad512, format self-describing)")
		singleWALMode  = flag.String("single-wal-mode", "", "12.14: explicit store layout: \"\"(auto)|unified|ref (ref = raft.log pointer meta + redo.log data, dual-fd single enter)")
		sparseIdx      = flag.Bool("sparse-index", false, "12.7: LSN->(index,off) sparse index (effective on single-wal files)")
		singleWALCodec = flag.String("single-wal-codec", "", "12.14: redo codec injection (\"\"=payload passthrough | index=stamps lsn=raft index)")
		fsmAsync       = flag.Bool("fsm-async-persist", false, "12.7: BatchingFSM.ApplyBatch pure bookkeeping (no Sync)")
		rpcRaw         = flag.Bool("rpc-raw-bytes", false, "12.8.2: raw-bytes GetLogRaw fast path")
		rpcWritev      = flag.Bool("rpc-writev", false, "12.8.3: writev/net.Buffers fan-out")
		rpcZCopy       = flag.Bool("rpc-zerocopy", false, "12.8.5: sendfile/splice zero-copy snapshot path")
		rpcBusyPoll    = flag.Bool("rpc-busy-poll", false, "12.8.6: SO_BUSY_POLL on accepted conns")
		rpcIORing      = flag.Bool("rpc-ioring", false, "12.8.7: io_uring network probe")
		rpcLevel       = flag.Int("rpc-protocol-level", 0, "12.8.8: protocol tier 0=auto/off 1=baseline 2=dual 3=QUIC 4=RDMA (reserved)")
		tcpNoDelay     = flag.Bool("tcp-nodelay", true, "12.8.4: TCP_NODELAY (default on)")
		tcpSndBuf      = flag.Int("tcp-sndbuf", 0, "12.8.4: SO_SNDBUF bytes (0=kernel default)")
		tcpUserTimeout = flag.Int("tcp-user-timeout", 0, "12.8.4: TCP_USER_TIMEOUT ms (0=off)")
		tcpBBR         = flag.Bool("tcp-bbr", false, "12.8.4: TCP_CONGESTION=bbr")
		snapThresh     = flag.Uint64("snapshot-threshold", 8192, "logs outstanding before a snapshot is triggered")
		noSnap         = flag.Bool("no-snapshot", false, "disable periodic snapshots (clean benchmark runs)")
		maxAppend      = flag.Int("max-append-entries", 64, "max AppendEntries in a single batch")
		leaderLease    = flag.Duration("leader-lease", 0, "leader lease timeout (0 uses raft default)")
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

	// File-backed stores (library-level filestore package: 12.7/12.9/12.14).
	logPath := filepath.Join(*dataDir, "logs", "raft.log")
	// 只认 raft.log 是否存在: 它同时是单 WAL 下的 redo 段文件, 是这个节点
	// "曾经属于某个集群"的最简判据, 决定后面走自举还是入群。
	hadData := fileExists(logPath)
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}
	var codecOpt filestore.RedoCodec
	switch *singleWALCodec {
	case "":
	case "index":
		codecOpt = filestore.IndexLSNCodec{}
	default:
		log.Fatalf("unknown -single-wal-codec %q (\"\"|index)", *singleWALCodec)
	}
	fsOpts := filestore.Options{
		SingleWAL:           *singleWAL,
		Mode:                filestore.Mode(*singleWALMode),
		SparseIndex:         *sparseIdx,
		Codec:               codecOpt,
		Fdatasync:           *useFdatasync,
		PreallocateSegments: *preallocSeg,
		SegmentSize:         *segSize,
		UseFadvise:          *useFadvise,
		UseSyncFileRange:    *useSFR,
		IORing:              *ioring,
		IORingSQPoll:        *ioringSQPoll,
		DirectIO:            *directIO,
		Logger:              log.Default(),
	}
	logStore, err := filestore.Open(filepath.Join(*dataDir, "logs"), fsOpts)
	if err != nil {
		log.Fatalf("open log store: %v", err)
	}
	stableStore, err := filestore.OpenStable(filepath.Join(*dataDir, "stable"))
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
		// 1<<32 而不是"无限大": 阈值的类型是 uint64, 用一个远大于任何基准规模的
		// 值即可让快照永不触发, 又不会引入回绕或特殊值; 24h 让周期触发也失效。
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
	if *tcpNoDelay {
		v := true
		conf.RPCTCPConfig.TCPNoDelay = &v
	} else {
		v := false
		conf.RPCTCPConfig.TCPNoDelay = &v
	}
	if *tcpSndBuf > 0 {
		v := *tcpSndBuf
		conf.RPCTCPConfig.SndBuf = &v
	}
	if *tcpUserTimeout > 0 {
		v := *tcpUserTimeout
		conf.RPCTCPConfig.UserTimeout = &v
	}
	if *tcpBBR {
		v := true
		conf.RPCTCPConfig.BBR = &v
	}

	var trans raft.Transport
	// 只有这些开关需要带配置的传输层; 全默认时走 NewTCPTransport 的简写形式。
	// 注意: -tcp-nodelay=false 不在这个表达式里, 所以它单独使用时不会切到
	// NewTCPTransportWithConfig, 也就是被静默忽略(见汇报)。
	needNTC := *rpcRaw || *rpcWritev || *rpcZCopy || *rpcBusyPoll || *rpcIORing || *rpcLevel != 0 || *tcpSndBuf > 0 || *tcpUserTimeout > 0 || *tcpBBR
	if needNTC {
		// MaxPool=4 / Timeout=10s 与下面 else 分支的简写形式刻意保持一致, 这样
		// "开了 RPC 开关"与"没开"只差在具体能力上, 连接池与超时口径不变。
		ntc := &raft.NetworkTransportConfig{MaxPool: 4, Timeout: 10 * time.Second, RPCRawBytesEnabled: *rpcRaw, RPCWritevEnabled: *rpcWritev, RPCZeroCopyEnabled: *rpcZCopy, RPCBusyPollEnabled: *rpcBusyPoll, RPCIORingEnabled: *rpcIORing, RPCProtocolLevel: *rpcLevel, RPCTCPConfig: raft.TransportRPCTCPConfig{TCPNoDelay: conf.RPCTCPConfig.TCPNoDelay, KeepAlive: conf.RPCTCPConfig.KeepAlive, SndBuf: conf.RPCTCPConfig.SndBuf, UserTimeout: conf.RPCTCPConfig.UserTimeout, BBR: conf.RPCTCPConfig.BBR}}
		if tr, err2 := raft.NewTCPTransportWithConfig(*raftAddr, adv, ntc); err2 != nil {
			log.Fatalf("open raft transport: %v", err2)
		} else {
			trans = tr
		}
	} else {
		if tr, err2 := raft.NewTCPTransport(*raftAddr, adv, 4, 10*time.Second, os.Stderr); err2 != nil {
			log.Fatalf("open raft transport: %v", err2)
		} else {
			trans = tr
		}
	}

	// A fresh node either bootstraps itself (first member) or joins an existing
	// cluster. A node with on-disk data just restarts and recovers its role.
	if !hadData {
		// 三条分支互斥: 自举要写创世配置; 入群这里只记一条日志(真正的入群在后台
		// goroutine 里做); 两者都没给就直接退出——空白节点没有第三条路。
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
		store:        logStore,
		advertise:    advertiseAddr,
		httpAddr:     *httpAddr,
		batchWindow:  *batchWin,
		asyncPersist: *asyncPers,
	}
	srv := node.httpServer()

	// Join the cluster if requested. Do this in the background: the cluster may
	// still be electing, and /join retries until the leader accepts us.
	if !hadData && *joinAddrs != "" {
		// 后台跑: 集群可能还在选举, joinCluster 会一直重试; 放前台会挡住 HTTP
		// 监听, 让别的节点连不上我们。
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

// fileExists 报告路径是否存在; 任何 Stat 错误(含权限不足)都算"不存在"。
// fileExists reports whether a path exists.
func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// makeAdvertiseAddr 把 "host:port" 形式的对外地址转成交给 NewTCPTransport 的 net.Addr:
// IP 字面量解析成 *net.TCPAddr, 主机名则包成 HostnameAddr 以免把解析结果固化下来。
// makeAdvertiseAddr turns a "host:port" advertise string into the net.Addr to
// hand to NewTCPTransport. IP literals resolve to *net.TCPAddr; hostnames are
// wrapped in raft.NewHostnameAddr so raft stores the DNS name rather than a
// potentially-stale IP.
//
// 算法解读：
//
//	原理：判据是 net.ParseIP(host) 是否为 nil——解析得出 IP 就说明这是一个具体
//	      地址, 用 ResolveTCPAddr 把它定死; 解析不出就当作主机名, 交给
//	      NewHostnameAddr 原样保留。
//	关键不变量：raft 配置里存的是这个 Addr 的字符串形式, 成员之间也用它互连。
//	      若把 k8s 的 StatefulSet DNS 名在启动时解析成 Pod IP 再存进去, Pod 重建
//	      后 IP 变化, 集群配置就会指向一个已经不存在的地址——保留主机名正是为了
//	      让地址在 Pod 重建后依然有效。
//	代价与边界：主机名不做任何可达性校验, 写错的名字要到实际建连时才会暴露;
//	      另外 0.0.0.0 能解析成 IP 从而走 ResolveTCPAddr 分支, 但它不可对外广播,
//	      会被 NewTCPTransport 拒绝(见 main 里的说明)。
//	设计权衡：用"能否解析成 IP"而不是显式开关来分流, 是因为命令行只需要一个
//	      地址字符串; 代价是这个判据对 "127.0.0.1" 这类写法同样成立(符合预期),
//	      但对 "localhost" 会走主机名分支(也符合预期)。
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

// joinCluster 依次请给定的 HTTP 地址把本节点加为 voter, 直到某个 leader 应答为止。
// joinCluster asks the given HTTP addresses (in order) to add this node as a
// voter. It retries until a leader acknowledges the join. Only the leader can
// AddVoter, so non-leader responses are ignored and we try the next address.
//
// 算法解读：
//
//	原理：双层循环——外层是"一轮", 内层按顺序试每个候选地址; 一轮里只要有一个
//	      地址返回成功就立即返回, 整轮都失败则 sleep 2s 再来一轮。这是个**永久
//	      重试**的循环, 没有次数上限也没有总超时。
//	关键不变量：判据只有 httpJoin 的返回错误——非 200、连不上、body 读失败都算
//	      失败, 于是"对方不是 leader"(同样是非 200)与"对方还没起来"被一视同仁地
//	      当成"换个地址再试", 这正是它不需要区分 leader 的原因。空字符串地址被
//	      跳过, 所以 -join 里的多余逗号不会卡住循环。
//	代价与边界：永久重试意味着一个永远进不了群的节点会安静地一直转下去(每 2s 一
//	      轮), 只靠日志判断进度; 调用方是 go joinCluster(...), 所以它不会阻塞启动。
//	      2s 是"比选举超时略长"的经验值, 保证下一轮大概率能碰上已经选出的 leader。
//	设计权衡：不自带退避、不设上限, 是因为演示程序假设 -join 给的地址就是集群成员,
//	      迟早会有 leader; 换成生产场景需要加指数退避与放弃条件。
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
