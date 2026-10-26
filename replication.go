// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-metrics/compat"
)

// 复制重试的退避参数: maxFailureScale 是指数上限 —— 10ms << 12 ≈ 41s 的最坏重试
// 间隔, 既压住对不可达 peer 的重试风暴, 又保证 peer 恢复后能在分钟量级内重连;
// failureWait 是退避基数(首次失败后的等待), 之后按 2^failures 递增。
const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	// ErrLogNotFound indicates a given log entry is not available.
	ErrLogNotFound = errors.New("log not found")

	// ErrPipelineReplicationNotSupported can be returned by the transport to
	// signal that pipeline replication is not supported in general, and that
	// no error message should be produced.
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

// followerReplication is in charge of sending snapshots and log entries from
// this leader during this particular term to a remote follower.
type followerReplication struct {
	// currentTerm and nextIndex must be kept at the top of the struct so
	// they're 64 bit aligned which is a requirement for atomic ops on 32 bit
	// platforms.

	// currentTerm is the term of this leader, to be included in AppendEntries
	// requests.
	currentTerm uint64

	// nextIndex 由复制 goroutine 写、其它 goroutine 经 atomic 读写, 因此必须与
	// currentTerm 一起放在结构体最前以保证 64 位对齐(见上面的说明)。
	//
	// nextIndex is the index of the next log entry to send to the follower,
	// which may fall past the end of the log.
	nextIndex uint64

	// matched is the highest log index confirmed stored by the follower (vraft
	// 12.5). Unlike nextIndex it only advances on real acks, so it is a reliable
	// catchup indicator for learner promotion (matched >= leader lastIndex ⇒ the
	// learner holds every committed entry). Initialized to 0 and updated on
	// successful AppendEntries and InstallSnapshot.
	matched uint64

	// noElide (12.14 P4): sticky per-follower switch that disables pointer-tier
	// elision after the follower reports DataMissingFrom (its redo file lacks
	// the bytes). Cleared when a request containing elided entries succeeds.
	noElide bool

	// peer contains the network address and ID of the remote follower.
	peer Server
	// peerLock protects 'peer'
	peerLock sync.RWMutex

	// commitment tracks the entries acknowledged by followers so that the
	// leader's commit index can advance. It is updated on successful
	// AppendEntries responses.
	commitment *commitment

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster. In the follower removed case, it carries a log
	// index; replication should be attempted with a best effort up through that
	// index, before exiting.
	stopCh chan uint64

	// triggerCh 同为容量 1 的脏标记: 复制例程每轮都重新读 lastLogIndex 全量追平,
	// 所以丢信号是安全的 —— 只可能少一次唤醒, 不会漏日志。
	//
	// triggerCh is notified every time new entries are appended to the log.
	triggerCh chan struct{}

	// triggerDeferErrorCh is used to provide a backchannel. By sending a
	// deferErr, the sender can be notified when the replication is done.
	triggerDeferErrorCh chan *deferError

	// lastContact 由各复制/心跳 goroutine 写, 由主线程的租约检查读, 故必须加锁。
	//
	// lastContact is updated to the current time whenever any response is
	// received from the follower (successful or not). This is used to check
	// whether the leader should step down (Raft.checkLeaderLease()).
	lastContact time.Time
	// lastContactLock protects 'lastContact'.
	lastContactLock sync.RWMutex

	// failures 由复制 goroutine 独占(heartbeat 另有一份局部计数), 因此无需原子
	// 操作; 成功一次即清零, 它决定下一次失败的重试退避档位。
	//
	// failures counts the number of failed RPCs since the last success, which is
	// used to apply backoff.
	failures uint64

	// notifyCh 是容量 1 的「脏标记」通道(由 asyncNotifyCh 投递, 满了即丢):
	// 连续的强制心跳请求会被合并成一次, 生产者永远不会被阻塞。
	//
	// notifyCh is notified to send out a heartbeat, which is used to check that
	// this server is still leader.
	notifyCh chan struct{}
	// notify is a map of futures to be resolved upon receipt of an
	// acknowledgement, then cleared from this map.
	notify map[*verifyFuture]struct{}
	// notifyLock protects 'notify'.
	notifyLock sync.Mutex

	// stepDown is used to indicate to the leader that we
	// should step down based on information from a follower.
	stepDown chan struct{}

	// allowPipeline is used to determine when to pipeline the AppendEntries RPCs.
	// It is private to this replication goroutine.
	allowPipeline bool
}

// notifyAll 以「leader 是否仍有效」的判定, 一次性唤醒所有等待中的 verifyFuture。
// 先把待唤醒集合整表摘走再投票(缩短持锁时间), 因此并发到达的新 future 不会被这次
// 投票误伤。
//
// notifyAll is used to notify all the waiting verify futures
// if the follower believes we are still the leader.
func (s *followerReplication) notifyAll(leader bool) {
	// Clear the waiting notifies minimizing lock time
	s.notifyLock.Lock()
	n := s.notify
	s.notify = make(map[*verifyFuture]struct{})
	s.notifyLock.Unlock()

	// Submit our votes
	for v := range n {
		v.vote(leader)
	}
}

// cleanNotify 把一个已解出的 verifyFuture 从待唤醒集合里摘除。
// 主线程在确认多数派同意或发现新 leader 后, 会遍历所有 followerReplication 调用
// 它, 否则这些 map 里会长期留着已不再需要唤醒的 future。
//
// cleanNotify is used to delete notify, .
func (s *followerReplication) cleanNotify(v *verifyFuture) {
	s.notifyLock.Lock()
	delete(s.notify, v)
	s.notifyLock.Unlock()
}

// LastContact 返回最近一次收到该 follower 应答的时刻(读写锁保护)。
//
// LastContact returns the time of last contact.
func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

// setLastContact 把最后联系时间刷新为当前时刻 —— 每次收到应答(无论成败)都要调,
// 否则 leader 会误判租约过期而退位。
//
// setLastContact sets the last contact to the current time.
func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

// replicate 是每个 follower 一条的长期复制例程: 在「普通模式(RPC 标签段)」与
// 「流水线模式(PIPELINE)」之间来回切换, 直到退位、被移出集群或进程关闭。
//
// 算法解读：
//
//	原理：RPC 段是一个 select 循环, 三个触发源(triggerCh / CommitTimeout 随机定时
//	  / triggerDeferErrorCh)都归约到同一个动作 —— 取当前 lastLogIndex 后
//	  replicateTo 追平; 复制健康(allowPipeline)时 goto PIPELINE 切到流水线,
//	  流水线一出错就退回 RPC 段重新对齐。
//	关键不变量：triggerCh 是容量 1 的「脏标记」而非计数信号 —— 生产者用
//	  asyncNotifyCh 投递, 满了就丢。丢掉的只是「还有新日志」这个提示本身, 而
//	  replicateTo 每轮都重新取 lastLogIndex 全量追平, 所以丢多少次都安全;
//	  这样生产者永远不会因为复制 goroutine 卡在磁盘 IO 上而被阻塞。
//	代价与边界：用 goto 而不是函数调用, 是为了让 RPC/PIPELINE 两段共享局部变量;
//	  代价是可读性 —— 所有 return 都在 close(stopHeartbeat) 的 defer 覆盖之下。
//	设计权衡：心跳单独跑在 heartbeat goroutine 而不在这里, 因为本循环可能长时间
//	  阻塞在磁盘 IO 上, 心跳若跟着卡住 follower 就会选举超时。
//
// replicate is a long running routine that replicates log entries to a single
// follower.
func (r *Raft) replicate(s *followerReplication) {
	// Start an async heartbeating routing
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

RPC:
	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx := r.getReplicationLastIndex()
			shouldStop = r.replicateTo(s, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("replication failed"))
			}
		case <-s.triggerCh:
			lastLogIdx := r.getReplicationLastIndex()
			shouldStop = r.replicateTo(s, lastLogIdx)
		// This is _not_ our heartbeat mechanism but is to ensure
		// followers quickly learn the leader's commit index when
		// raft commits stop flowing naturally. The actual heartbeats
		// can't do this to keep them unblocked by disk IO on the
		// follower. See https://github.com/hashicorp/raft/issues/282.
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx := r.getReplicationLastIndex()
			shouldStop = r.replicateTo(s, lastLogIdx)
		}

		// If things looks healthy, switch to pipeline mode
		if !shouldStop && s.allowPipeline {
			goto PIPELINE
		}
	}
	return

PIPELINE:
	// Disable until re-enabled
	s.allowPipeline = false

	// Replicates using a pipeline for high performance. This method
	// is not able to gracefully recover from errors, and so we fall back
	// to standard mode on failure.
	if err := r.pipelineReplicate(s); err != nil {
		if err != ErrPipelineReplicationNotSupported {
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()
			r.logger.Error("failed to start pipeline replication to", "peer", peer, "error", err)
		}
	}
	goto RPC
}

// replicateTo 把日志从 s.nextIndex 追到 lastIndex: 逐轮发 AppendEntries, 被拒就
// 回退 nextIndex 重试, 取不到日志则改发快照; 返回是否应当停止复制。
//
// 算法解读：
//
//	原理：START 标签构成重试环 —— setupAppendEntries 取不到 prevLog/entries 就
//	  goto SEND_SNAP(改发快照), 成功则推进 nextIndex 回到 START 继续补;
//	  被 follower 拒绝则把 nextIndex 回退到 min(nextIndex-1, resp.LastLog+1)
//	  (下限 1)后重试, 直到 nextIndex > lastIndex。
//	关键不变量：nextIndex 的回退有下界(不小于 1)且正常每轮至少退一步 —— 否则
//	  follower 的拒绝理由与 leader 的推断不一致时会原地重试; 退到 1 之后仍被拒就
//	  不再回退, 此时只靠 failures 累积的退避压住重试速率。失败计数只在 follower
//	  显式声明 NoRetryBackoff 时才清零, 否则累积以拉开退避档位。
//	代价与边界：失败重试带 backoff(failureWait=10ms 起, 上限 2^maxFailureScale);
//	  CHECK_MORE 每轮都探一次 stopCh, 避免对一个正在退出的节点无意义地狂发日志。
//	设计权衡：本函数只返回 bool 而不返回 error —— 出错一律记日志并靠外层循环重试,
//	  因为复制是「尽力而为的常驻任务」, 没有调用方能对错误做有意义的处理。
//
// replicateTo is a helper to replicate(), used to replicate the logs up to a
// given last index.
// If the follower log is behind, we take care to bring them up to date.
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	// Create the base request
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	var start time.Time
	var peer Server

START:
	// Prevent an excessive retry rate on errors
	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	s.peerLock.RLock()
	peer = s.peer
	s.peerLock.RUnlock()

	// Setup the request.
	//
	// ★#105-B(2026-09-25): 这里原为 `err == ErrLogNotFound` —— **身份比较**。而
	// filestore.GetLog 在「位置算术命中了别的条目」时(索引空洞, 见 filestore.go 的
	// hole detected 分支)是 `fmt.Errorf("... : %w", raft.ErrLogNotFound)` **包装**返回的。
	// 包装后的 error 与哨兵不是同一个值, 身份比较为假 ⇒ 取不到日志**只走到
	// `else if err != nil { return }`**, SEND_SNAP 永远不可达。
	//
	// 后果(实测, logdb-tps 41k 表档, 2026-09-25): 一个索引空洞让领导者每轮都在同一个
	// 索引上重试 fail-loud, 快照回退这条**唯一的自愈路径**被哨兵错配挡在门外, 而 raft
	// 又无法自己制造缺失的那一条 ⇒ 复制永久冻结, commit 停在空洞之下, 调用侧
	// ApplyRedoBatchBase 在 commit 上无界等待(见 LogDBStorage raft_node.go 的退避修复)。
	// 现场读数: s0 的 raft.log 只有 17 条(18716–18732)而 commit=18765、lastIdx=18732,
	// 日志里每秒 ~2 行 `raft: failed to get log: index=18714 error="log not found"`。
	//
	// errors.Is 对**未包装的哨兵**与身份比较逐字节等价(哨兵本身既是链头也是链尾),
	// 因此这条改动对「GetLog 直接返回哨兵」这一通常路径零行为差异, 只把包装形态一并认下。
	if err := r.setupAppendEntries(s, &req, atomic.LoadUint64(&s.nextIndex), lastIndex); errors.Is(err, ErrLogNotFound) {
		goto SEND_SNAP
	} else if err != nil {
		return
	}

	// Make the RPC call
	start = time.Now()
	if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
		r.logger.Error("failed to appendEntries to", "peer", peer, "error", err)
		s.failures++
		return
	}
	appendStats(string(peer.ID), start, float32(len(req.Entries)), r.noLegacyTelemetry)

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true
	}

	// Update the last contact
	s.setLastContact()

	// Update s based on success
	if resp.Success {
		// Update our replication state
		updateLastAppended(s, &req)

		// vraft: per-peer replication gap = leader lastLog − follower lastLog.
		// High/rising gap means the follower is falling behind (or we should send
		// a snapshot); used by the adaptive tuning loop (12.6).
		lastLogIdx, _ := r.getLastLog()
		labels := []metrics.Label{{Name: "peer_id", Value: string(peer.ID)}}
		metrics.SetGaugeWithLabels([]string{"raft", "leader", "replicationGap"}, float32(int64(lastLogIdx)-int64(resp.LastLog)), labels)

		// Clear any failures, allow pipelining
		s.failures = 0
		s.allowPipeline = true
		// 12.14 P4: a success on a request that carried elided entries clears
		// the sticky full-data mode for this follower.
		if s.noElide {
			for _, e := range req.Entries {
				if e != nil && DecodeElideMarker(e.Extensions) != nil {
					s.noElide = false
					break
				}
			}
		}
	} else {
		// 12.14 P4: follower lacks the redo bytes for elided entries — switch
		// this follower to full-data replication until a pointer-tier retry
		// succeeds again.
		if resp.DataMissingFrom > 0 {
			if !s.noElide {
				r.logger.Warn("follower missing redo bytes for elided entries, falling back to full data",
					"peer", peer, "from_index", resp.DataMissingFrom)
				metrics.IncrCounter([]string{"raft", "replication", "elide", "fallback"}, 1)
			}
			s.noElide = true
		}
		atomic.StoreUint64(&s.nextIndex, max(min(s.nextIndex-1, resp.LastLog+1), 1))
		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
		r.logger.Warn("appendEntries rejected, sending older logs", "peer", peer, "next", atomic.LoadUint64(&s.nextIndex))
	}

CHECK_MORE:
	// Poll the stop channel here in case we are looping and have been asked
	// to stop, or have stepped down as leader. Even for the best effort case
	// where we are asked to replicate to a given index and then shutdown,
	// it's better to not loop in here to send lots of entries to a straggler
	// that's leaving the cluster anyways.
	select {
	case <-s.stopCh:
		return true
	default:
	}

	// Check if there are more logs to replicate
	if atomic.LoadUint64(&s.nextIndex) <= lastIndex {
		goto START
	}
	return

	// SEND_SNAP is used when we fail to get a log, usually because the follower
	// is too far behind, and we must ship a snapshot down instead
SEND_SNAP:
	// 走到这里说明 prevLog 取不到 —— 通常是 follower 太落后, 日志已被截断或压实,
	// 唯一能补齐的路径就是把本地快照整份送过去。
	if stop, err := r.sendLatestSnapshot(s); stop {
		return true
	} else if err != nil {
		r.logger.Error("failed to send snapshot to", "peer", peer, "error", err)
		return
	}

	// Check if there is more to replicate
	goto CHECK_MORE
}

// sendLatestSnapshot 把本地最新的快照整份推给 follower; 成功后把该 follower 的
// nextIndex/matched 直接跳到快照索引之后。
//
// 算法解读：
//
//	原理：List() 取快照列表并约定 [0] 即最新; Open 后经 InstallSnapshot 把整份
//	  快照流式送出; 成功后 nextIndex = meta.Index+1、matched = meta.Index,
//	  并向 commitment 报告该 follower 已匹配到快照点。
//	关键不变量：失败方向是**保守的** —— 列表为空、Open 失败、RPC 失败一律返回
//	  (false, err), 即「不要停止复制」, 让上层退回 RPC 段重试; 只有应答的 Term
//	  大于本请求的 Term 才返回 (true, nil) 要求停止(本节点已非 leader)。
//	代价与边界：快照传输期间 nextIndex 不动, 若快照过大复制会长时间停滞; 失败只
//	  累加 failures, 不做任何补救(不退位、不换快照)。
//	设计权衡：不在这里退位 —— 发送失败可能只是该 follower 暂时不可达, 与 leader
//	  合法性无关; 只有 Term 比较才能判定 leader 失效。
//
// sendLatestSnapshot is used to send the latest snapshot we have
// down to our follower.
func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {
	// Get the snapshots
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Error("failed to list snapshots", "error", err)
		return false, err
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// Open the most recent snapshot
	snapID := snapshots[0].ID
	r.logger.Info("opening snapshot", "id", snapID)
	meta, snapshot, err := r.snapshots.Open(snapID)
	if err != nil {
		r.logger.Error("failed to open snapshot", "id", snapID, "error", err)
		return false, err
	}
	defer func() { _ = snapshot.Close() }()

	// Setup the request
	req := InstallSnapshotRequest{
		RPCHeader:       r.getRPCHeader(),
		SnapshotVersion: meta.Version,
		Term:            s.currentTerm,
		// this is needed for retro compatibility, before RPCHeader.Addr was added
		Leader:             r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      EncodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	r.logger.Info("installing snapshot on", "peer", peer.ID, "id", snapID, "size", req.Size)
	// Make the call
	start := time.Now()
	var resp InstallSnapshotResponse
	if err := r.trans.InstallSnapshot(peer.ID, peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Error("failed to install snapshot", "peer", peer.ID, "id", snapID, "error", err)
		s.failures++
		return false, err
	}
	labels := []metrics.Label{{Name: "peer_id", Value: string(peer.ID)}}
	metrics.MeasureSinceWithLabels([]string{"raft", "replication", "installSnapshot"}, start, labels)

	if !r.noLegacyTelemetry {
		// Duplicated information. Kept for backward compatibility.
		metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(peer.ID)}, start)
	}

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true, nil
	}

	// Update the last contact
	s.setLastContact()

	// Check for success
	if resp.Success {
		// Update the indexes
		atomic.StoreUint64(&s.nextIndex, meta.Index+1)
		atomic.StoreUint64(&s.matched, meta.Index)
		s.commitment.match(peer.ID, meta.Index)

		// Clear any failures
		s.failures = 0

		// Notify we are still leader
		s.notifyAll(true)
	} else {
		s.failures++
		r.logger.Warn("installSnapshot rejected to", "peer", peer.ID, "id", snapID)
	}
	return false, nil
}

// heartbeat 在独立 goroutine 里周期性向 follower 发空 AppendEntries 防止其选举
// 超时; 与 replicate 分家正是因为它不能被磁盘 IO 拖住。
//
// 算法解读：
//
//	原理：等待 notifyCh(强制心跳)、HeartbeatTimeout/10 的随机定时、或 stopCh;
//	  唤醒后发一条空 AppendEntries, 并用应答的 Success 去解所有 verifyFuture
//	  —— 线性一致读(verifyFuture)就是借这条心跳通道确认领导权的。
//	关键不变量：notifyCh 是容量 1 的脏标记(asyncNotifyCh), 连续多次请求合并成一次
//	  心跳; 心跳失败只做指数退避重试, **不主动退位** —— 退位由主线程的 leader
//	  lease 检查依据 lastContact 统一裁决, 避免多条 goroutine 各自退位。
//	代价与边界：退避上限是 HeartbeatTimeout/2, 保证失败后仍能在选举超时内恢复联系;
//	  每次失败上报 FailedHeartbeatObservation, 恢复后上报 Resumed。
//	设计权衡：不复用 replicate 的连接与状态, 换来「心跳永不被日志复制阻塞」;
//	  代价是每个 follower 多一条 goroutine 与一次 RPC 往返。
//
// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      s.currentTerm,
		// this is needed for retro compatibility, before RPCHeader.Addr was added
		Leader: r.trans.EncodePeer(r.localID, r.localAddr),
	}

	var resp AppendEntriesResponse
	for {
		// Wait for the next heartbeat interval or forced notify
		select {
		case <-s.notifyCh:
		case <-randomTimeout(r.config().HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}

		s.peerLock.RLock()
		peer := s.peer
		s.peerLock.RUnlock()

		start := time.Now()
		if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
			nextBackoffTime := cappedExponentialBackoff(failureWait, failures, maxFailureScale, r.config().HeartbeatTimeout/2)
			r.logger.Error("failed to heartbeat to", "peer", peer.Address, "backoff time",
				nextBackoffTime, "error", err)
			r.observe(FailedHeartbeatObservation{PeerID: peer.ID, LastContact: s.LastContact()})
			failures++
			select {
			case <-time.After(nextBackoffTime):
			case <-stopCh:
				return
			}
		} else {
			if failures > 0 {
				r.observe(ResumedHeartbeatObservation{PeerID: peer.ID})
			}
			s.setLastContact()
			failures = 0
			labels := []metrics.Label{{Name: "peer_id", Value: string(peer.ID)}}
			metrics.MeasureSinceWithLabels([]string{"raft", "replication", "heartbeat"}, start, labels)

			if !r.noLegacyTelemetry {
				// Duplicated information. Kept for backward compatibility.
				metrics.MeasureSince([]string{"raft", "replication", "heartbeat", string(peer.ID)}, start)
			}

			s.notifyAll(resp.Success)
		}
	}
}

// pipelineReplicate 在状态已追平后改用流水线模式高速复制: 不等待应答地连发
// AppendEntries, 由独立的 pipelineDecode goroutine 消化应答; 一旦出错即返回,
// 由 replicate 退回普通模式。
//
// 算法解读：
//
//	原理：pipelineSend 只管发(把 nextIdx 本地推到已发条目的下一条, 不等应答),
//	  pipelineDecode 在另一条 goroutine 里按序消费应答并调用 updateLastAppended;
//	  「发」与「确认」解耦, 从而把 RPC 往返时延从吞吐路径上摘掉。
//	关键不变量：流水线只在**已追平**时启用 —— nextIndex 必须已对齐, 因为流水线内
//	  nextIdx 只增不减、无法回退; 这正是「一出错就整体退回普通模式」的根本原因:
//	  回退 nextIndex、发快照、退避这些动作全都只在 replicateTo 里才有。
//	代价与边界：无退避、不能发快照; 任何 resp.Success=false 或 Term 过期都直接
//	  结束流水线, 由外层 goto RPC 重新以普通模式对齐。
//	设计权衡：用独立流水线而不是给普通模式加并发 —— 普通模式的复杂度(回退、快照、
//	  退避)与高吞吐互相拉扯, 分成两条路径各自才能保持简单。
//
// pipelineReplicate is used when we have synchronized our state with the follower,
// and want to switch to a higher performance pipeline mode of replication.
// We only pipeline AppendEntries commands, and if we ever hit an error, we fall
// back to the standard replication which can handle more complex situations.
func (r *Raft) pipelineReplicate(s *followerReplication) error {
	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	// Create a new pipeline
	pipeline, err := r.trans.AppendEntriesPipeline(peer.ID, peer.Address)
	if err != nil {
		return err
	}
	defer func() { _ = pipeline.Close() }()

	// Log start and stop of pipeline
	r.logger.Info("pipelining replication", "peer", peer)
	defer r.logger.Info("aborting pipeline replication", "peer", peer)

	// Create a shutdown and finish channel
	stopCh := make(chan struct{})
	finishCh := make(chan struct{})

	// Start a dedicated decoder
	r.goFunc(func() { r.pipelineDecode(s, pipeline, stopCh, finishCh) })

	// Start pipeline sends at the last good nextIndex
	nextIndex := atomic.LoadUint64(&s.nextIndex)

	shouldStop := false
SEND:
	for !shouldStop {
		select {
		case <-finishCh:
			break SEND
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.pipelineSend(s, pipeline, &nextIndex, maxIndex)
			}
			break SEND
		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx := r.getReplicationLastIndex()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("replication failed"))
			}
		case <-s.triggerCh:
			lastLogIdx := r.getReplicationLastIndex()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx := r.getReplicationLastIndex()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		}
	}

	// Stop our decoder, and wait for it to finish
	close(stopCh)
	select {
	case <-finishCh:
	case <-r.shutdownCh:
	}
	return nil
}

// pipelineSend 通过流水线发出一次 AppendEntries(不等应答), 并把 nextIdx 前推到
// 已发送条目的下一条; 任一步失败即返回 true 要求退出流水线。
//
// pipelineSend is used to send data over a pipeline. It is a helper to
// pipelineReplicate.
func (r *Raft) pipelineSend(s *followerReplication, p AppendPipeline, nextIdx *uint64, lastIndex uint64) (shouldStop bool) {
	// Create a new append request
	req := new(AppendEntriesRequest)
	if err := r.setupAppendEntries(s, req, *nextIdx, lastIndex); err != nil {
		return true
	}

	// Pipeline the append entries
	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
		r.logger.Error("failed to pipeline appendEntries", "peer", s.peer, "error", err)
		return true
	}

	// Increase the next send log to avoid re-sending old logs
	// 注意 nextIdx 只增不回退: 流水线里没有回退的余地, 失败只能靠退出流水线解决。
	if n := len(req.Entries); n > 0 {
		last := req.Entries[n-1]
		atomic.StoreUint64(nextIdx, last.Index+1)
	}
	return false
}

// pipelineDecode 在独立 goroutine 里消费流水线应答: 刷新 lastContact 并推进复制
// 进度; 遇到 Term 过期或应答失败立即返回, 使 pipelineReplicate 退出流水线。
//
// pipelineDecode is used to decode the responses of pipelined requests.
func (r *Raft) pipelineDecode(s *followerReplication, p AppendPipeline, stopCh, finishCh chan struct{}) {
	defer close(finishCh)
	respCh := p.Consumer()
	for {
		select {
		case ready := <-respCh:
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()

			req, resp := ready.Request(), ready.Response()
			appendStats(string(peer.ID), ready.Start(), float32(len(req.Entries)), r.noLegacyTelemetry)

			// Check for a newer term, stop running
			if resp.Term > req.Term {
				r.handleStaleTerm(s)
				return
			}

			// Update the last contact
			s.setLastContact()

			// Abort pipeline if not successful
			if !resp.Success {
				return
			}

			// Update our replication state
			updateLastAppended(s, req)
		case <-stopCh:
			return
		}
	}
}

// setupAppendEntries 组装一条 AppendEntries 请求: RPC 头 + leaderCommitIndex +
// prevLog 锚点 + 待复制 entries(必要时做 P4 指针化省略)。
//
// 算法解读：
//
//	原理：先 setPreviousLog 定位 follower 端的一致性锚点(prevLogIndex/Term),
//	  再 setNewLogs 取出 [nextIndex, min(nextIndex+MaxAppendEntries-1, lastIndex)]
//	  区间; 最后在 RefReplicationEnabled 且该 follower 未被标记 noElide 时,
//	  把 redo 条目的 Data 换成指针(经 LogDataElider)。
//	关键不变量：两个 set* 任一失败都必须原样上抛 —— 上层据此 goto SEND_SNAP
//	  改发快照(取不到 prevLog 说明 follower 落后太多, 日志已被截断或压实)。
//	代价与边界：MaxAppendEntries 封顶单次批量, 防止一次请求撑爆 MTU 或内存;
//	  省略 Data 会让 follower 需要本地 redo 才能还原, 还原不了就回退全量。
//	设计权衡：省略放在最后一步(entries 已就位之后)而不是塞进 getLog, 这样
//	  「取日志」与「上不上指针」正交, 回退全量时取值路径一行都不用改。
//
// setupAppendEntries is used to setup an append entries request.
func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	req.RPCHeader = r.getRPCHeader()
	req.Term = s.currentTerm
	// this is needed for retro compatibility, before RPCHeader.Addr was added
	req.Leader = r.trans.EncodePeer(r.localID, r.localAddr)
	req.LeaderCommitIndex = r.getCommitIndex()
	if err := r.setPreviousLog(req, nextIndex); err != nil {
		return err
	}
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil {
		return err
	}
	// 12.14 P4: pointer-tier elision. When the store supports it (ref mode) and
	// this follower hasn't recently reported missing bytes, redo entries go out
	// with Data elided (pointer in Extensions). A follower that can't resolve
	// answers DataMissingFrom>0 → we stick to full data for it (noElide) until
	// a pointer-tier request succeeds again.
	if r.config().RefReplicationEnabled && !s.noElide {
		if elider, ok := r.logs.(LogDataElider); ok {
			for i, e := range req.Entries {
				if e == nil || len(e.Data) == 0 || len(e.Extensions) > 0 {
					continue
				}
				if el, elided := elider.ElideLogData(e); elided {
					req.Entries[i] = el
					metrics.IncrCounter([]string{"raft", "replication", "elide"}, 1)
				}
			}
		}
	}
	return nil
}

// setPreviousLog 依 nextIndex 定位 prevLog 锚点: 首条(index==1)用 (0,0), 紧接快照
// 之后用快照的 (index,term), 其余情况读日志里的 nextIndex-1 条。
//
// 算法解读：
//
//	原理：prevLogIndex/prevLogTerm 是 follower 侧的一致性校验锚点 —— follower 只在
//	  自己的 (prevLogIndex, prevLogTerm) 与本地日志相符时才接受后续 entries,
//	  否则回退; 因此锚点必须精确指向 nextIndex-1 那一条。
//	关键不变量：nextIndex==1 与「上一条恰好是快照」这两种边界**不能去读日志**:
//	  前者没有第 0 条, 后者的条目可能已被 Truncate 掉; 漏掉任一特判都会让首次
//	  复制或快照之后的复制直接失败。
//	代价与边界：非边界路径每轮多一次 GetLog 读盘; 冲突截断由 follower 侧完成
//	  (leader 不删自己的日志, 只靠回退 nextIndex 重新对齐)。
//	设计权衡：锚点由 leader 按 nextIndex 推算而非等 follower 上报 —— 省掉一轮
//	  协商, 代价是 leader 必须准确维护每个 follower 的 nextIndex。
//
// setPreviousLog is used to setup the PrevLogEntry and PrevLogTerm for an
// AppendEntriesRequest given the next index to replicate.
func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {
	// Guard for the first index, since there is no 0 log entry
	// Guard against the previous index being a snapshot as well
	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0

	} else if (nextIndex - 1) == lastSnapIdx {
		req.PrevLogEntry = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm

	} else {
		var l Log
		// vraft: read via the pending-aware getLog so a follower whose
		// nextIndex lands inside the async-persist window still sees the
		// previous entry.
		if err := r.getLog(nextIndex-1, &l); err != nil {
			r.logger.Error("failed to get log", "index", nextIndex-1, "error", err)
			return err
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

// setNewLogs 取出本轮要发的日志区间 [nextIndex, min(nextIndex+MaxAppendEntries-1,
// lastIndex)]; RPCRawBytesEnabled 时走 GetLogRaw 的免解码快路径。
//
// 算法解读：
//
//	原理：区间上界由 MaxAppendEntries 与 lastIndex 共同封顶 —— 既限制单次批量
//	  大小, 又不越过 leader 已有的日志末尾。
//	关键不变量：容量按 maxAppendEntries 预分配, 但只 append 区间内的条目; raw 路径
//	  构造的是「最小合成 Log」(仅 Index+Data), Term/Type/Extensions 靠二次 getLog
//	  补齐, 因此 raw 路径必须保证 Data 已是最终线格式。
//	代价与边界：raw 快路径失败(GetLogRaw 报错)会静默落回通用路径; 区间为空
//	  (nextIndex > lastIndex)时返回空 entries —— 这既是心跳, 也是「无新日志」。
//	设计权衡：不在这里做指针化省略(见 setupAppendEntries) —— 保持「取值」纯粹,
//	  使 raw 路径与省略路径可以独立开关、独立回退。
//
// setNewLogs is used to setup the logs which should be appended for a request.
func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// 12.8.2: when RPCRawBytesEnabled and the log store exposes GetLogRaw, clone
	// the entry's Data slice directly (no extra decode pass). Falls back to the
	// legacy getLog path; keeps the wire payload identical.
	useRaw := r.config().RPCRawBytesEnabled
	maxAppendEntries := r.config().MaxAppendEntries
	req.Entries = make([]*Log, 0, maxAppendEntries)
	maxIndex := min(nextIndex+uint64(maxAppendEntries)-1, lastIndex)
	for i := nextIndex; i <= maxIndex; i++ {
		if useRaw {
			if s, ok := r.logs.(interface{ GetLogRaw(uint64) ([]byte, error) }); ok {
				raw, err := s.GetLogRaw(i)
				if err == nil {
					// Minimal synthetic Log: Index is authoritative for replication,
					// Data is already wire-ready; Term is best-effort (filled below
					// if missing).
					oldLog := &Log{Index: i, Data: raw}
					if oldLog.Term == 0 {
						var tmp Log
						if err2 := r.getLog(i, &tmp); err2 == nil {
							oldLog.Term = tmp.Term
							oldLog.Type = tmp.Type
							oldLog.Extensions = tmp.Extensions
						}
					}
					req.Entries = append(req.Entries, oldLog)
					continue
				}
			}
		}
		oldLog := new(Log)
		if err := r.getLog(i, oldLog); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// appendStats 上报一次 AppendEntries 的往返耗时与条目数; skipLegacy 为真时只发
// 带 peer_id 标签的新指标, 跳过旧格式的重复指标。
//
// appendStats is used to emit stats about an AppendEntries invocation.
func appendStats(peer string, start time.Time, logs float32, skipLegacy bool) {
	labels := []metrics.Label{{Name: "peer_id", Value: peer}}
	metrics.MeasureSinceWithLabels([]string{"raft", "replication", "appendEntries", "rpc"}, start, labels)
	metrics.IncrCounterWithLabels([]string{"raft", "replication", "appendEntries", "logs"}, logs, labels)

	if !skipLegacy {
		// Duplicated information. Kept for backward compatibility.
		metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", peer}, start)
		metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", peer}, logs)
	}
}

// handleStaleTerm 在 follower 回报更高 term 时退位: 把所有等待中的 verifyFuture
// 判为「已非 leader」, 并异步通知 stepDown。
//
// 算法解读：
//
//	原理：应答里的 Term 大于本请求的 Term, 说明集群中已存在更新的任期 —— 本节点
//	  无论如何不可能继续做 leader, 于是立刻放弃领导权。
//	关键不变量：必须**先** notifyAll(false) 再触发 stepDown —— 等待中的
//	  verifyFuture(线性一致读)必须先被判定失败, 否则它们会一直挂到超时。
//	代价与边界：用 asyncNotifyCh 通知 stepDown(非阻塞), 因为本函数跑在复制
//	  goroutine 上, 不能等主循环把退位处理完; 通知之后本 goroutine 直接结束复制。
//	设计权衡：退位不做任何「补发/挽回」尝试 —— 更高 term 是权威事实, 任何重试都
//	  只会白费一次 RPC 往返并推迟真正需要的重新选举。
//
// handleStaleTerm is used when a follower indicates that we have a stale term.
func (r *Raft) handleStaleTerm(s *followerReplication) {
	r.logger.Error("peer has newer term, stopping replication", "peer", s.peer)
	s.notifyAll(false) // No longer leader
	asyncNotifyCh(s.stepDown)
}

// updateLastAppended 在一次成功的 AppendEntries 之后推进该 follower 的
// nextIndex/matched, 并向 commitment 报告其匹配进度。
//
// updateLastAppended is used to update follower replication state after a
// successful AppendEntries RPC.
// TODO: This isn't used during InstallSnapshot, but the code there is similar.
func updateLastAppended(s *followerReplication, req *AppendEntriesRequest) {
	// Mark any inflight logs as committed
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&s.nextIndex, last.Index+1)
		atomic.StoreUint64(&s.matched, last.Index)
		// 注意：这里直接读 s.peer.ID 而未持 peerLock —— UpdatePeer 会并发改写该
		// 字段(raft.go 里在 peerLock 下写), 属于潜在数据竞争; 后果最多是匹配进度
		// 记到旧的 key 上, 不会损坏日志(commitment.match 对不在 map 里的 ID 会忽略)。
		s.commitment.match(s.peer.ID, last.Index)
	}

	// Notify still leader
	s.notifyAll(true)
}
