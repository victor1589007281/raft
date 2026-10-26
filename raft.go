// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hashicorp/go-metrics/compat"
)

const (
	minCheckInterval          = 10 * time.Millisecond // 租约检查的最小间隔: 防止 checkInterval 被算成 0 或负数而忙等
	oldestLogGaugeInterval    = 10 * time.Second
	rpcUnexpectedCommandError = "unexpected command"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

// getRPCHeader 构造本节点的 RPCHeader(协议版本 + 本地 ID + 编码后的地址), 随每个 RPC 请求/响应一起发送。
// getRPCHeader returns an initialized RPCHeader struct for the given
// Raft instance. This structure is sent along with RPC requests and
// responses.
func (r *Raft) getRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: r.config().ProtocolVersion,
		ID:              []byte(r.config().LocalID),
		Addr:            r.trans.EncodePeer(r.config().LocalID, r.localAddr),
	}
}

// checkRPCHeader 校验入站 RPC 的协议版本是否可处理: 先看是否落在 [ProtocolVersionMin, ProtocolVersionMax] 内, 再看是否 >= 本节点配置版本-1(只向后兼容一个大版本)。
// checkRPCHeader houses logic about whether this instance of Raft can process
// the given RPC message.
func (r *Raft) checkRPCHeader(rpc RPC) error {
	// Get the header off the RPC message.
	wh, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC does not have a header")
	}
	header := wh.GetRPCHeader()

	// First check is to just make sure the code can understand the
	// protocol at all.
	if header.ProtocolVersion < ProtocolVersionMin ||
		header.ProtocolVersion > ProtocolVersionMax {
		return ErrUnsupportedProtocol
	}

	// Second check is whether we should support this message, given the
	// current protocol we are configured to run. This will drop support
	// for protocol version 0 starting at protocol version 2, which is
	// currently what we want, and in general support one version back. We
	// may need to revisit this policy depending on how future protocol
	// changes evolve.
	if header.ProtocolVersion < r.config().ProtocolVersion-1 {
		return ErrUnsupportedProtocol
	}

	return nil
}

// getSnapshotVersion 返回当前协议版本下应使用的快照版本; 现有两个版本互相兼容, 故恒返回 1。
// getSnapshotVersion returns the snapshot version that should be used when
// creating snapshots, given the protocol version in use.
func getSnapshotVersion(protocolVersion ProtocolVersion) SnapshotVersion {
	// Right now we only have two versions and they are backwards compatible
	// so we don't need to look at the protocol version.
	return 1
}

// commitTuple is used to send an index that was committed,
// with an optional associated future that should be invoked.
type commitTuple struct {
	log    *Log
	future *logFuture
}

// asyncWrite is a single leader batch queued for (serial) persistence by the
// async persist writer goroutine.
type asyncWrite struct {
	logs       []*Log
	futures    []*logFuture
	lastIndex  uint64
	term       uint64
	start      time.Time
	commitment *commitment // captured at dispatch; only used if still leader of w.term
}

// asyncStoreErr 返回导致异步持久化写者失败的错误; 未记录具体错误时回退为一个通用错误。
// asyncStoreErr returns the error that caused the async persist writer to
// fail, or a generic one if none is recorded.
func (r *Raft) asyncStoreErr() error {
	if v := r.asyncWriteErr.Load(); v != nil {
		if e, ok := v.(error); ok {
			return e
		}
	}
	return fmt.Errorf("async log store failure")
}

// asyncStoreErrStore 记录使异步持久化写者停摆的错误(原子写, 供主线程读取)。
// asyncStoreErrStore records the error that stopped the async persist writer.
func (r *Raft) asyncStoreErrStore(err error) {
	r.asyncWriteErr.Store(err)
}

// leaderState is state that is used while we are a leader.
type leaderState struct {
	leadershipTransferInProgress int32 // indicates that a leadership transfer is in progress.
	commitCh                     chan struct{}
	commitment                   *commitment
	inflight                     *list.List // list of logFuture in log index order
	replState                    map[ServerID]*followerReplication
	notify                       map[*verifyFuture]struct{}
	stepDown                     chan struct{}

	// vraft (12.4): the leadership noop is held here instead of being
	// dispatched immediately, so it can be merged into the first real batch
	// and save one replication round for the first committed write. It is only
	// touched by the main thread. nil means no pending noop.
	pendingNoop *logFuture
}

// setLeader 更新当前已知 leader 的地址与 ID, 并在两者任一变化时对外发出 LeaderObservation。
// setLeader is used to modify the current leader Address and ID of the cluster
func (r *Raft) setLeader(leaderAddr ServerAddress, leaderID ServerID) {
	r.leaderLock.Lock()
	oldLeaderAddr := r.leaderAddr
	r.leaderAddr = leaderAddr
	oldLeaderID := r.leaderID
	r.leaderID = leaderID
	r.leaderLock.Unlock()
	if oldLeaderAddr != leaderAddr || oldLeaderID != leaderID {
		r.observe(LeaderObservation{Leader: leaderAddr, LeaderAddr: leaderAddr, LeaderID: leaderID})
	}
}

// requestConfigChange 把成员变更请求投递给主线程, 按 timeout 超时(<=0 表示不设超时); 关机中返回 ErrRaftShutdown。
// requestConfigChange is a helper for the above functions that make
// configuration change requests. 'req' describes the change. For timeout,
// see AddVoter.
func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &configurationChangeFuture{
		req: req,
	}
	future.init()
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

// run 是 Raft 的主线程循环: 按当前状态分派到 runFollower / runCandidate / runLeader, 关机时先清空 leader 再返回。
// run the main thread that handles leadership and RPC requests.
func (r *Raft) run() {
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			// Clear the leader to prevent forwarding
			r.setLeader("", "")
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower 跑 follower 状态的主循环: 处理入站 RPC, 拒绝一切写/配置/校验请求, 并在心跳超时后转为 candidate。
// runFollower runs the main loop while in the follower state.
func (r *Raft) runFollower() {
	didWarn := false
	// campaignDelayApplied 标记【本次 leader 失联周期】是否已经应用过 CampaignDelay。
	// 只延迟一次: 延迟到期后即使仍未联系上 leader, 也会正常进入 candidate 选举,
	// 因此该机制不可能永久推迟选举(可用性边界见 Config.CampaignDelay 注释)。
	campaignDelayApplied := false
	leaderAddr, leaderID := r.LeaderWithID()
	r.logger.Info("entering follower state", "follower", r, "leader-address", leaderAddr, "leader-id", leaderID)
	metrics.IncrCounter([]string{"raft", "state", "follower"}, 1)
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		r.mainThreadSaturation.sleeping()

		select {
		case rpc := <-r.rpcCh:
			r.mainThreadSaturation.working()
			r.processRPC(rpc)

		case c := <-r.configurationChangeCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			v.respond(ErrNotLeader)

		case ur := <-r.userRestoreCh:
			r.mainThreadSaturation.working()
			// Reject any restores since we are not the leader
			ur.respond(ErrNotLeader)

		case l := <-r.leadershipTransferCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			l.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			r.mainThreadSaturation.working()
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case cf := <-r.catchupCh:
			r.mainThreadSaturation.working()
			// Not the leader, cannot satisfy a catchup poll.
			cf.respond(ErrNotLeader)

		case b := <-r.bootstrapCh:
			r.mainThreadSaturation.working()
			b.respond(r.liveBootstrap(b.configuration))

		case <-r.leaderNotifyCh:
			//  Ignore since we are not the leader

		case <-r.followerNotifyCh:
			heartbeatTimer = time.After(0)

		case <-heartbeatTimer:
			r.mainThreadSaturation.working()
			// Restart the heartbeat timer
			hbTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(hbTimeout)

			// Check if we have had a successful contact
			lastContact := r.LastContact() // 只认心跳窗口内的联系: 距上次成功联系超过一个心跳超时即认为 leader 失联
			if time.Since(lastContact) < hbTimeout {
				// leader 又联系上了: 复位延迟标记, 下次真正失联时允许重新评估一次。
				campaignDelayApplied = false
				continue
			}

			// Heartbeat failed! Transition to the candidate state
			// soft B: 前沿加权的候选延迟 —— 仅延迟、不拒绝。leader 失联时若调用方
			// 声明需要延迟(例如本节点 redo 前沿落后, 让更新的节点先竞选), 则把心跳
			// 定时器改为延迟到期, continue 回 select 继续处理入站 RPC/心跳; 延迟期间
			// 若重新收到 leader 心跳, 上面的分支会复位标记, 不会竞选。
			// 注意: 这里绝不 sleep 阻塞主循环, 也绝不阻止延迟到期后的正常选举。
			if !campaignDelayApplied && r.config().CampaignDelay != nil {
				d := r.config().CampaignDelay()
				if d > 0 {
					if d > 2*time.Second {
						d = 2 * time.Second // 硬上限: 保证可用性, 最坏只多等 2s
					}
					campaignDelayApplied = true
					metrics.IncrCounter([]string{"raft", "campaign", "delay_applied"}, 1)
					r.logger.Info("delaying campaign for redo frontier", "delay", d)
					heartbeatTimer = time.After(d)
					continue
				}
			}

			lastLeaderAddr, lastLeaderID := r.LeaderWithID()
			r.setLeader("", "")

			if r.configurations.latestIndex == 0 {
				if !didWarn {
					r.logger.Warn("no known peers, aborting election")
					didWarn = true
				}
			} else if r.configurations.latestIndex == r.configurations.committedIndex &&
				!hasVote(r.configurations.latest, r.localID) {
				if !didWarn {
					r.logger.Warn("not part of stable configuration, aborting election")
					didWarn = true
				}
			} else {
				metrics.IncrCounter([]string{"raft", "transition", "heartbeat_timeout"}, 1)
				if hasVote(r.configurations.latest, r.localID) {
					r.logger.Warn("heartbeat timeout reached, starting election", "last-leader-addr", lastLeaderAddr, "last-leader-id", lastLeaderID)
					r.setState(Candidate)
					return
				} else if !didWarn {
					r.logger.Warn("heartbeat timeout reached, not part of a stable configuration or a non-voter, not triggering a leader election")
					didWarn = true
				}
			}

		case <-r.shutdownCh:
			return
		}
	}
}

// liveBootstrap 为集群播种初始配置(在线版 BootstrapCluster), 只能在主线程且仅当处于 follower 状态时调用。
// liveBootstrap attempts to seed an initial configuration for the cluster. See
// the Raft object's member BootstrapCluster for more details. This must only be
// called on the main thread, and only makes sense in the follower state.
func (r *Raft) liveBootstrap(configuration Configuration) error {
	if !hasVote(configuration, r.localID) {
		// Reject this operation since we are not a voter
		return ErrNotVoter
	}

	// Use the pre-init API to make the static updates.
	cfg := r.config()
	err := BootstrapCluster(&cfg, r.logs, r.stable, r.snapshots, r.trans, configuration)
	if err != nil {
		return err
	}

	// Make the configuration live.
	var entry Log
	if err := r.logs.GetLog(1, &entry); err != nil {
		panic(err)
	}
	r.setCurrentTerm(1)
	r.setLastLog(entry.Index, entry.Term)
	return r.processConfigurationLogEntry(&entry)
}

// runCandidate 跑 candidate 状态的主循环: 先征集 pre-vote(除非被禁用或本次是领导权转移), 拿到法定票数后当选, 否则等选举超时重来。
// runCandidate runs the main loop while in the candidate state.
//
// 算法解读：
//
//	原理：默认先走预选举 —— 以 currentTerm+1 征集 pre-vote, 攒够法定票数才真正进入正式选举(electSelf, 会
//	递增任期); 若被禁用预选举、或本次由领导权转移触发(candidateFromLeadershipTransfer), 则直接正式选举。
//	关键不变量：收到更高任期的 pre-vote/vote 一律回退为 follower 并接受该任期; 转移触发的竞选跳过预选举是
//	刻意的(否则转移会被预选举的「已有 leader」判据挡回); candidateFromLeadershipTransfer 每次退出 runCandidate
//	都用 defer 复位, 否则该特权会被滥用(带此标志的 RequestVote 会让已有所属的节点也投票)。
//	代价与边界：预选举多一轮往返, 但能在被隔离的节点上避免任期无意义地膨胀, 也就避免了它回来时打断健康
//	leader。
//	设计权衡：预选举把「试探」与「真投票」分开, 代价是选举慢一轮; 换来的是隔离节点不会靠自增任期反复扰乱
//	集群。
func (r *Raft) runCandidate() {
	term := r.getCurrentTerm() + 1
	r.logger.Info("entering candidate state", "node", r, "term", term)
	metrics.IncrCounter([]string{"raft", "state", "candidate"}, 1)

	// vraft (12.4): timestamp the start of the campaign (pre-vote included).
	// runLeader measures into raft.election.elapsed once the election is won.
	r.electionStartedAt = time.Now()

	// Start vote for us, and set a timeout
	var voteCh <-chan *voteResult
	var prevoteCh <-chan *preVoteResult

	// check if pre-vote is active and that this is not a leader transfer.
	// Leader transfer do not perform prevote by design
	if !r.preVoteDisabled && !r.candidateFromLeadershipTransfer.Load() {
		prevoteCh = r.preElectSelf()
	} else {
		voteCh = r.electSelf()
	}

	// Make sure the leadership transfer flag is reset after each run. Having this
	// flag will set the field LeadershipTransfer in a RequestVoteRequest to true,
	// which will make other servers vote even though they have a leader already.
	// It is important to reset that flag, because this privilege could be abused
	// otherwise.
	defer func() { r.candidateFromLeadershipTransfer.Store(false) }()

	electionTimeout := r.config().ElectionTimeout
	electionTimer := randomTimeout(electionTimeout)

	// Tally the votes, need a simple majority
	preVoteGrantedVotes := 0
	preVoteRefusedVotes := 0
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	r.logger.Debug("calculated votes needed", "needed", votesNeeded, "term", term)

	for r.getState() == Candidate {
		r.mainThreadSaturation.sleeping()

		select {
		case rpc := <-r.rpcCh:
			r.mainThreadSaturation.working()
			r.processRPC(rpc)
		case preVote := <-prevoteCh:
			// This a pre-vote case it should trigger a "real" election if the pre-vote is won.
			r.mainThreadSaturation.working()
			r.logger.Debug("pre-vote received", "from", preVote.voterID, "term", preVote.Term, "tally", preVoteGrantedVotes)
			// Check if the term is greater than ours, bail
			if preVote.Term > term {
				r.logger.Debug("pre-vote denied: found newer term, falling back to follower", "term", preVote.Term)
				r.setState(Follower)
				r.setCurrentTerm(preVote.Term)
				return
			}

			// Check if the preVote is granted
			if preVote.Granted {
				preVoteGrantedVotes++
				r.logger.Debug("pre-vote granted", "from", preVote.voterID, "term", preVote.Term, "tally", preVoteGrantedVotes)
			} else {
				preVoteRefusedVotes++
				r.logger.Debug("pre-vote denied", "from", preVote.voterID, "term", preVote.Term, "tally", preVoteGrantedVotes)
			}

			// Check if we've won the pre-vote and proceed to election if so
			if preVoteGrantedVotes >= votesNeeded {
				r.logger.Info("pre-vote successful, starting election", "term", preVote.Term,
					"tally", preVoteGrantedVotes, "refused", preVoteRefusedVotes, "votesNeeded", votesNeeded)
				preVoteGrantedVotes = 0
				preVoteRefusedVotes = 0
				electionTimer = randomTimeout(electionTimeout)
				prevoteCh = nil
				voteCh = r.electSelf()
			}
			// Check if we've lost the pre-vote and wait for the election to timeout so we can do another time of
			// prevote.
			if preVoteRefusedVotes >= votesNeeded {
				r.logger.Info("pre-vote campaign failed, waiting for election timeout", "term", preVote.Term,
					"tally", preVoteGrantedVotes, "refused", preVoteRefusedVotes, "votesNeeded", votesNeeded)
			}
		case vote := <-voteCh:
			r.mainThreadSaturation.working()
			// Check if the term is greater than ours, bail
			if vote.Term > r.getCurrentTerm() {
				r.logger.Debug("newer term discovered, fallback to follower", "term", vote.Term)
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			// Check if the vote is granted
			if vote.Granted {
				grantedVotes++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.Term, "tally", grantedVotes)
			}

			// Check if we've become the leader
			if grantedVotes >= votesNeeded {
				r.logger.Info("election won", "term", vote.Term, "tally", grantedVotes)
				r.setState(Leader)
				r.setLeader(r.localAddr, r.localID)
				return
			}
		case c := <-r.configurationChangeCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			v.respond(ErrNotLeader)

		case ur := <-r.userRestoreCh:
			r.mainThreadSaturation.working()
			// Reject any restores since we are not the leader
			ur.respond(ErrNotLeader)

		case l := <-r.leadershipTransferCh:
			r.mainThreadSaturation.working()
			// Reject any operations since we are not the leader
			l.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			r.mainThreadSaturation.working()
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case cf := <-r.catchupCh:
			r.mainThreadSaturation.working()
			// Not the leader, cannot satisfy a catchup poll.
			cf.respond(ErrNotLeader)

		case b := <-r.bootstrapCh:
			r.mainThreadSaturation.working()
			b.respond(ErrCantBootstrap)

		case <-r.leaderNotifyCh:
			//  Ignore since we are not the leader

		case <-r.followerNotifyCh:
			if electionTimeout != r.config().ElectionTimeout {
				electionTimeout = r.config().ElectionTimeout
				electionTimer = randomTimeout(electionTimeout)
			}

		case <-electionTimer:
			r.mainThreadSaturation.working()
			// Election failed! Restart the election. We simply return,
			// which will kick us back into runCandidate
			r.logger.Warn("Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// setLeadershipTransferInProgress 以原子方式设置「领导权转移进行中」标志, 供任意 goroutine 读取。
func (r *Raft) setLeadershipTransferInProgress(v bool) {
	if v {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 1)
	} else {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 0)
	}
}

// getLeadershipTransferInProgress 原子读取「领导权转移进行中」标志。
func (r *Raft) getLeadershipTransferInProgress() bool {
	v := atomic.LoadInt32(&r.leaderState.leadershipTransferInProgress)
	return v == 1
}

// setupLeaderState 初始化本次任期专属的 leader 状态(commitCh / commitment / inflight / replState / notify / stepDown), 并在开启 AsyncLeaderPersist 时拉起每任期的异步持久化写者。
func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh,
		r.configurations.latest,
		r.getLastIndex()+1, /* first index that may be committed in this term */
		r.localID)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.notify = make(map[*verifyFuture]struct{})
	r.leaderState.stepDown = make(chan struct{}, 1)

	// Start the per-leadership async persist writer. The writer serially
	// drains the leader's log batches to the store; replicators are notified
	// at dispatch time, so their work overlaps the leader's own fsync. A fresh
	// term resets the failed flag so a transient store failure does not
	// permanently wedge later leadership terms. The config assertion is guarded
	// so setupLeaderState remains callable on a config-less Raft (some tests
	// exercise it directly with a bare struct).
	if cfg, ok := r.conf.Load().(Config); ok && cfg.AsyncLeaderPersist {
		r.asyncWriteFailed.Store(false)
		r.asyncWriteStop = make(chan struct{})
		r.goFunc(func() { r.runAsyncWriter(r.asyncWriteStop) })
	}
}

// runLeader 进入 leader 状态的主入口: 完成领导权通知、leader 状态初始化、租约清零、后台指标协程与下台清理 defer, 然后落入 leaderLoop 热循环。
// runLeader runs the main loop while in leader state. Do the setup here and drop into
// the leaderLoop for the hot loop.
//
// 算法解读：
//
//	原理：新 leader 必须先提交一条本任期的条目(noop)才能建立任期提交点。这里不单独派发 noop, 而是把它挂
//	到 leaderState.pendingNoop, 等 leaderLoop 收到第一条真实日志时并到该批最前面一起派发; 同时起一个
//	ElectionTimeout/2 的兜底定时器, 超时仍无真实日志就单独派发 noop。
//	关键不变量：noop 必须在本任期被提交 —— 兜底定时器保证空闲集群不会永远等不到; pendingNoop 只由主线程
//	读写, 下台时被置 nil(它从未拿到索引, 丢弃无副作用)。
//	代价与边界：合并省下一次复制往返(第一条真实写入少一轮 RPC); 代价是空闲集群的任期提交点最多被推迟
//	ElectionTimeout/2。
//	设计权衡：为什么取选举超时的一半? noop 只是为了让本任期能提交, 对空闲集群没有延迟压力; 半程既远小于
//	一次选举超时(不会诱发新选举), 又给真实写入留出足够的「搭车」窗口。合并只对任期内的第一批生效。
func (r *Raft) runLeader() {
	r.logger.Info("entering leader state", "leader", r)
	metrics.IncrCounter([]string{"raft", "state", "leader"}, 1)

	// Notify that we are the leader
	overrideNotifyBool(r.leaderCh, true)

	// Store the notify chan. It's not reloadable so shouldn't change before the
	// defer below runs, but this makes sure we always notify the same chan if
	// ever for both gaining and losing leadership.
	notify := r.config().NotifyCh

	// Push to the notify channel if given
	if notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
			// make sure push to the notify channel ( if given )
			select {
			case notify <- true:
			default:
			}
		}
	}

	// setup leader state. This is only supposed to be accessed within the
	// leaderloop.
	r.setupLeaderState()

	// The lease is earned by contacting a quorum, never inherited from a prior
	// leadership. Reset the deadline so stale contact from an earlier term cannot
	// authorize a lease read.
	r.leaderLeaseExpiry.Store(0)

	// Run a background go-routine to emit metrics on log age
	stopCh := make(chan struct{})
	go emitLogStoreMetrics(r.logs, []string{"raft", "leader"}, oldestLogGaugeInterval, stopCh)

	// Cleanup state on step down
	defer func() {
		close(stopCh)

		// Since we were the leader previously, we update our
		// last contact time when we step down, so that we are not
		// reporting a last contact time from before we were the
		// leader. Otherwise, to a client it would seem our data
		// is extremely stale.
		r.setLastContact()

		// Stop replication
		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}

		// Respond to all inflight operations
		for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
			e.Value.(*logFuture).respond(ErrLeadershipLost)
		}

		// Stop the async persist writer and drop its pending buffer: entries
		// that were dispatched but never made it to disk must not be served by
		// a future leader under the same index range.
		if r.asyncWriteStop != nil {
			close(r.asyncWriteStop)
			r.asyncWriteStop = nil
		}
		r.clearPending()

		// Respond to any pending verify requests
		for future := range r.leaderState.notify {
			future.respond(ErrLeadershipLost)
		}

		// The lease dies with the leadership: without this reset a lost leader
		// could keep serving lease reads until the stale deadline passes.
		r.leaderLeaseExpiry.Store(0)

		// Drop any undispased leadership noop: it was never assigned a log
		// index, so a future leadership starts clean.
		r.leaderState.pendingNoop = nil

		// Clear all the state
		r.leaderState.commitCh = nil
		r.leaderState.commitment = nil
		r.leaderState.inflight = nil
		r.leaderState.replState = nil
		r.leaderState.notify = nil
		r.leaderState.stepDown = nil

		// If we are stepping down for some reason, no known leader.
		// We may have stepped down due to an RPC call, which would
		// provide the leader, so we cannot always blank this out.
		r.leaderLock.Lock()
		if r.leaderAddr == r.localAddr && r.leaderID == r.localID {
			r.leaderAddr = ""
			r.leaderID = ""
		}
		r.leaderLock.Unlock()

		// Notify that we are not the leader
		overrideNotifyBool(r.leaderCh, false)

		// Push to the notify channel if given
		if notify != nil {
			select {
			case notify <- false:
			case <-r.shutdownCh:
				// On shutdown, make a best effort but do not block
				select {
				case notify <- false:
				default:
				}
			}
		}
	}()

	// Start a replication routine for each peer
	r.startStopReplication()

	// Dispatch a no-op log entry first. This gets this leader up to the latest
	// possible commit index, even in the absence of client commands. This used
	// to append a configuration entry instead of a noop. However, that permits
	// an unbounded number of uncommitted configurations in the log. We now
	// maintain that there exists at most one uncommitted configuration entry in
	// any log, so we have to do proper no-ops here.
	//
	// vraft (12.4): the noop is held in leaderState.pendingNoop and merged into
	// the first real batch (leaderLoop) instead of being dispatched on its own,
	// saving one replication round for the first committed write. A fallback
	// timer flushes it alone if no client log arrives promptly, so the term
	// commit is still established in idle clusters.
	r.leaderState.pendingNoop = &logFuture{log: Log{Type: LogNoop}}

	// Measure the elapsed time of the winning election (set by runCandidate).
	if !r.electionStartedAt.IsZero() {
		metrics.MeasureSince([]string{"raft", "election", "elapsed"}, r.electionStartedAt)
		r.electionStartedAt = time.Time{}
	}

	// Sit in the leader loop until we step down
	r.leaderLoop()
}

// startStopReplication 按最新配置增删复制管线: 为新 peer 建 followerReplication 并起复制协程, 为被移除的 peer 复制到 lastIdx 后停掉; 只能在主线程调用。
// startStopReplication will set up state and start asynchronous replication to
// new peers, and stop replication to removed peers. Before removing a peer,
// it'll instruct the replication routines to try to replicate to the current
// index. This must only be called from the main thread.
//
// 算法解读：
//
//	原理：以最新配置为准做差集 —— 配置里有而 replState 里没有的 peer 新建 followerReplication 并起复制
//	协程(初始 nextIndex = lastIdx+1, 假定对方已跟上); replState 里有而配置里没有的 peer, 先让复制协程把
//	日志推到 lastIdx 再关闭 stopCh, 然后从 map 删除。
//	关键不变量：被移除的 peer 必须先「追到 lastIdx」再停 —— 直接停会让它缺一段日志, 若它随后又出现在配置
//	里就得走快照; 新增 peer 的 nextIndex 从 lastIdx+1 起步, 靠 AppendEntries 的冲突回退逐步校准到真实位置。
//	代价与边界：本函数在主线程同步执行, 每处理一次配置变更就全量扫一遍 latest 配置与 replState(成员数 n 时
//	O(n)), 成员规模大时开销可观。
//	设计权衡：不做增量 diff 维护而是每次全量比对 —— 配置变更本就低频, 换来「replState 与 latest 配置最终
//	一致」这一简单不变量。
func (r *Raft) startStopReplication() {
	inConfig := make(map[ServerID]bool, len(r.configurations.latest.Servers))
	lastIdx := r.getLastIndex()

	// Start replication goroutines that need starting
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}

		inConfig[server.ID] = true

		s, ok := r.leaderState.replState[server.ID]
		if !ok {
			r.logger.Info("added peer, starting replication", "peer", server.ID)
			s = &followerReplication{
				peer:                server,
				commitment:          r.leaderState.commitment,
				stopCh:              make(chan uint64, 1),
				triggerCh:           make(chan struct{}, 1),
				triggerDeferErrorCh: make(chan *deferError, 1),
				currentTerm:         r.getCurrentTerm(),
				nextIndex:           lastIdx + 1,
				lastContact:         time.Now(),
				notify:              make(map[*verifyFuture]struct{}),
				notifyCh:            make(chan struct{}, 1),
				stepDown:            r.leaderState.stepDown,
			}

			r.leaderState.replState[server.ID] = s
			r.goFunc(func() { r.replicate(s) })
			asyncNotifyCh(s.triggerCh)
			r.observe(PeerObservation{Peer: server, Removed: false})
		} else if ok {

			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()

			if peer.Address != server.Address {
				r.logger.Info("updating peer", "peer", server.ID)
				s.peerLock.Lock()
				s.peer = server
				s.peerLock.Unlock()
			}
		}
	}

	// Stop replication goroutines that need stopping
	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}
		// Replicate up to lastIdx and stop
		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIdx)
		repl.stopCh <- lastIdx
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
		r.observe(PeerObservation{Peer: repl.peer, Removed: true})
	}

	// Update peers metric
	metrics.SetGauge([]string{"raft", "peers"}, float32(len(r.configurations.latest.Servers)))
}

// configurationChangeChIfStable 仅在配置变更可安全处理时返回 configurationChangeCh, 否则返回 nil(在 select 中该分支永不就绪)。
// configurationChangeChIfStable returns r.configurationChangeCh if it's safe
// to process requests from it, or nil otherwise. This must only be called
// from the main thread.
//
// Note that if the conditions here were to change outside of leaderLoop to take
// this from nil to non-nil, we would need leaderLoop to be kicked.
func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {
	// Have to wait until:
	// 1. The latest configuration is committed, and
	// 2. This leader has committed some entry (the noop) in this term
	//    https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
}

// leaderLoop 是 leader 的热循环: 处理 RPC、推进提交、组提交攒批、租约检查与领导权转移。
// leaderLoop is the hot loop for a leader. It is invoked
// after all the various leader setup is done.
//
// 算法解读：
//
//	原理：收到第一条 apply 后, 若配置了 BatchWindow(w>0), leader 把它当作批次起点, 在窗口内继续从 applyCh
//	收集后续 append, 直到攒满 MaxAppendEntries 或窗口到期; 整批共享一次 StoreLogs 与一次复制往返。开启
//	BatchWindowAdaptive 时先只等 grace=max(w/10, 500µs) 看有没有第二条: 有则照常等满窗口, 没有(批仍只有 1
//	条)则跳过剩余窗口立即派发。
//	关键不变量：批次内所有条目按 applyCh 到达顺序拿到连续索引; 窗口只决定「何时派发」, 不改「派发顺序」
//	—— 攒批期间 leader 一直占着主线程, 不会有第二个攒批循环并发; pendingNoop 至多被合并一次(合并时置 nil
//	并停掉 flush 定时器)。
//	代价与边界：窗口越大吞吐越高(每批摊销的 fsync/往返越少), 但单条命令的追加延迟上界也越大(最坏 = 窗口
//	长度)。低载时若无免窗, 每条命令都要白等满窗口(固定延迟税 = w), 免窗把它压到 grace。窗口仅在 w>0 时
//	启用, 否则走上游的非阻塞 GROUP_COMMIT_LOOP(只捞已就绪条目, 不等待)。
//	设计权衡：用「时间窗」而非「固定批大小」, 是因为低并发下批永远填不满, 等大小会无限等待; 用自适应
//	grace 而非直接取消窗口, 是因为一旦第二条在 grace 内到达就说明有并发, 等满窗口仍划算 —— 免的只是
//	「独苗」那一种情形的税。
func (r *Raft) leaderLoop() {
	// stepDown is used to track if there is an inflight log that
	// would cause us to lose leadership (specifically a RemovePeer of
	// ourselves). If this is the case, we must not allow any logs to
	// be processed in parallel, otherwise we are basing commit on
	// only a single peer (ourself) and replicating to an undefined set
	// of peers.
	stepDown := false
	// This is only used for the first lease check, we reload lease below
	// based on the current config value.
	lease := time.After(r.config().LeaderLeaseTimeout) // 首次租约检查用当前配置的完整窗口, 之后按 checkLeaderLease 返回的 maxDiff 动态校准

	// vraft (12.4): the leadership noop is merged into the first real batch to
	// save one replication round. If no client log arrives within half the
	// election timeout, flush the noop alone so the term commit is still
	// established promptly in idle clusters. flushNoopCh is per-leadership, so a
	// stale timer from a previous leadership cannot touch the new one.
	flushNoopCh := make(chan struct{}, 1)
	var flushNoopTimer *time.Timer
	if r.leaderState.pendingNoop != nil {
		flushTimeout := r.config().ElectionTimeout / 2 // 取选举超时的一半: 远小于一次选举超时(不会诱发新选举), 又给真实写入留出搭车窗口
		flushNoopTimer = time.AfterFunc(flushTimeout, func() {
			select {
			case flushNoopCh <- struct{}{}:
			default:
			}
		})
	}

	for r.getState() == Leader {
		r.mainThreadSaturation.sleeping()

		select {
		case rpc := <-r.rpcCh:
			r.mainThreadSaturation.working()
			r.processRPC(rpc)

		case <-r.leaderState.stepDown:
			r.mainThreadSaturation.working()
			r.setState(Follower)

		case <-r.asyncPersistErrCh:
			r.mainThreadSaturation.working()
			r.logger.Error("async log persistence failed, stepping down")
			r.setState(Follower)

		case <-flushNoopCh:
			r.mainThreadSaturation.working()
			// Fallback timer fired before any real batch arrived: dispatch the
			// leadership noop on its own.
			if noop := r.leaderState.pendingNoop; noop != nil {
				r.leaderState.pendingNoop = nil
				if stepDown {
					noop.respond(ErrNotLeader)
				} else {
					r.dispatchLogs([]*logFuture{noop})
				}
			}

		case cf := <-r.catchupCh:
			r.mainThreadSaturation.working()
			// Learner catchup poll (vraft 12.5): report whether the server's
			// confirmed matched index has reached the leader's last log index.
			if r.getState() != Leader {
				cf.respond(ErrNotLeader)
				continue
			}
			repl, ok := r.leaderState.replState[cf.serverID]
			if !ok {
				cf.respond(fmt.Errorf("raft: no replication state for server %s", cf.serverID))
				continue
			}
			cf.caughtUp = atomic.LoadUint64(&repl.matched) >= r.getLastIndex() // 判据: 该 server 已确认匹配的索引追上了 leader 当前的 lastIndex
			cf.respond(nil)

		case future := <-r.leadershipTransferCh:
			r.mainThreadSaturation.working()
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}

			r.logger.Debug("starting leadership transfer", "id", future.ID, "address", future.Address)

			// When we are leaving leaderLoop, we are no longer
			// leader, so we should stop transferring.
			leftLeaderLoop := make(chan struct{})
			defer func() { close(leftLeaderLoop) }()

			stopCh := make(chan struct{})
			doneCh := make(chan error, 1)

			// This is intentionally being setup outside of the
			// leadershipTransfer function. Because the TimeoutNow
			// call is blocking and there is no way to abort that
			// in case eg the timer expires.
			// The leadershipTransfer function is controlled with
			// the stopCh and doneCh.
			// No matter how this exits, have this function set
			// leadership transfer to false before we return
			//
			// Note that this leaves a window where callers of
			// LeadershipTransfer() and LeadershipTransferToServer()
			// may start executing after they get their future but before
			// this routine has set leadershipTransferInProgress back to false.
			// It may be safe to modify things such that setLeadershipTransferInProgress
			// is set to false before calling future.Respond, but that still needs
			// to be tested and this situation mirrors what callers already had to deal with.
			go func() {
				defer r.setLeadershipTransferInProgress(false)
				select {
				case <-time.After(r.config().ElectionTimeout):
					close(stopCh)
					err := fmt.Errorf("leadership transfer timeout")
					r.logger.Debug(err.Error())
					future.respond(err)
					<-doneCh
				case <-leftLeaderLoop:
					close(stopCh)
					err := fmt.Errorf("lost leadership during transfer (expected)")
					r.logger.Debug(err.Error())
					future.respond(nil)
					<-doneCh
				case err := <-doneCh:
					if err != nil {
						r.logger.Debug(err.Error())
						future.respond(err)
					} else {
						// Wait for up to ElectionTimeout before flagging the
						// leadership transfer as done and unblocking applies in
						// the leaderLoop.
						select {
						case <-time.After(r.config().ElectionTimeout):
							err := fmt.Errorf("leadership transfer timeout")
							r.logger.Debug(err.Error())
							future.respond(err)
						case <-leftLeaderLoop:
							r.logger.Debug("lost leadership during transfer (expected)")
							future.respond(nil)
						}
					}
				}
			}()

			// leaderState.replState is accessed here before
			// starting leadership transfer asynchronously because
			// leaderState is only supposed to be accessed in the
			// leaderloop.
			id := future.ID
			address := future.Address
			if id == nil {
				s := r.pickServer()
				if s != nil {
					id = &s.ID
					address = &s.Address
				} else {
					doneCh <- fmt.Errorf("cannot find peer")
					continue
				}
			}
			state, ok := r.leaderState.replState[*id]
			if !ok {
				doneCh <- fmt.Errorf("cannot find replication state for %v", id)
				continue
			}
			r.setLeadershipTransferInProgress(true)
			go r.leadershipTransfer(*id, *address, state, stopCh, doneCh)

		case <-r.leaderState.commitCh:
			r.mainThreadSaturation.working()
			// Process the newly committed entries
			oldCommitIndex := r.getCommitIndex()
			commitIndex := r.leaderState.commitment.getCommitIndex()
			r.setCommitIndex(commitIndex)

			// New configuration has been committed, set it as the committed
			// value.
			if r.configurations.latestIndex > oldCommitIndex &&
				r.configurations.latestIndex <= commitIndex {
				r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
				if !hasVote(r.configurations.committed, r.localID) {
					stepDown = true
				}
			}

			start := time.Now()
			var groupReady []*list.Element
			groupFutures := make(map[uint64]*logFuture)
			var lastIdxInGroup uint64

			// Pull all inflight logs that are committed off the queue.
			for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
				commitLog := e.Value.(*logFuture)
				idx := commitLog.log.Index
				if idx > commitIndex {
					// Don't go past the committed index
					break
				}

				// Measure the commit time
				metrics.MeasureSince([]string{"raft", "commitTime"}, commitLog.dispatch)
				groupReady = append(groupReady, e)
				groupFutures[idx] = commitLog
				lastIdxInGroup = idx
			}

			// Process the group
			if len(groupReady) != 0 {
				r.processLogs(lastIdxInGroup, groupFutures)

				for _, e := range groupReady {
					r.leaderState.inflight.Remove(e)
				}
			}

			// Measure the time to enqueue batch of logs for FSM to apply
			metrics.MeasureSince([]string{"raft", "fsm", "enqueue"}, start)

			// Count the number of logs enqueued
			metrics.SetGauge([]string{"raft", "commitNumLogs"}, float32(len(groupReady)))

			if stepDown {
				if r.config().ShutdownOnRemove {
					r.logger.Info("removed ourself, shutting down")
					r.Shutdown()
				} else {
					r.logger.Info("removed ourself, transitioning to follower")
					r.setState(Follower)
				}
			}

		case v := <-r.verifyCh:
			r.mainThreadSaturation.working()
			if v.quorumSize == 0 {
				// Just dispatched, start the verification
				r.verifyLeader(v)
			} else if v.votes < v.quorumSize {
				// Early return, means there must be a new leader
				r.logger.Warn("new leader elected, stepping down")
				r.setState(Follower)
				delete(r.leaderState.notify, v)
				for _, repl := range r.leaderState.replState {
					repl.cleanNotify(v)
				}
				v.respond(ErrNotLeader)

			} else {
				// Quorum of members agree, we are still leader
				delete(r.leaderState.notify, v)
				for _, repl := range r.leaderState.replState {
					repl.cleanNotify(v)
				}
				v.respond(nil)
			}

		case future := <-r.userRestoreCh:
			r.mainThreadSaturation.working()
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			err := r.restoreUserSnapshot(future.meta, future.reader)
			future.respond(err)

		case future := <-r.configurationsCh:
			r.mainThreadSaturation.working()
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			future.configurations = r.configurations.Clone()
			future.respond(nil)

		case future := <-r.configurationChangeChIfStable():
			r.mainThreadSaturation.working()
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			r.appendConfigurationEntry(future)

		case b := <-r.bootstrapCh:
			r.mainThreadSaturation.working()
			b.respond(ErrCantBootstrap)

		case newLog := <-r.applyCh:
			r.mainThreadSaturation.working()
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				newLog.respond(ErrLeadershipTransferInProgress)
				continue
			}
			// Group commit, gather all the ready commits
			ready := []*logFuture{newLog}
			// vraft (12.4): merge the pending leadership noop into this first
			// real batch, saving one replication round for the first committed
			// write of the term.
			if noop := r.leaderState.pendingNoop; noop != nil {
				r.leaderState.pendingNoop = nil
				if flushNoopTimer != nil {
					flushNoopTimer.Stop()
				}
				ready = append([]*logFuture{noop}, ready...)
			}
			if w := r.config().BatchWindow; w > 0 {
				// vraft: time-window group commit. Hold the batch open for at most
				// BatchWindow (measured from the first queued entry) so that even a
				// low-concurrency writer can coalesce multiple appends into one batch
				// and pay one StoreLogs + one replication round per batch. Dispatch
				// as soon as the batch fills MaxAppendEntries or the window expires.
				skipWindow := false
				if r.config().BatchWindowAdaptive {
					// 低载免窗: 先给短 grace 等第二条, 无来活且批仍 1 条 → 跳过窗口
					// 立即派发(消除低并发固定窗口税); grace 内来活则照常等满窗口。
					grace := w / 10 // 免窗 grace = 窗口的十分之一
					if grace < 500*time.Microsecond {
						grace = 500 * time.Microsecond // 下限 500µs: 窗口很小时 w/10 会被舍成 0, 那就退化成不免窗了
					}
					gt := time.NewTimer(grace)
					select {
					case newLog := <-r.applyCh:
						ready = append(ready, newLog)
					case <-gt.C:
						skipWindow = len(ready) <= 1
					}
					gt.Stop()
					select {
					case <-gt.C:
					default:
					}
				}
				if !skipWindow {
					timer := time.NewTimer(w)
				GROUP_COMMIT_WINDOW:
					for i := 1; i < r.config().MaxAppendEntries; i++ {
						select {
						case newLog := <-r.applyCh:
							ready = append(ready, newLog)
						case <-timer.C:
							break GROUP_COMMIT_WINDOW
						}
					}
					timer.Stop()
					// Drain a fired timer so the channel is not left readable.
					select {
					case <-timer.C:
					default:
					}
				}
			} else {
				// Default behavior: dispatch after a non-blocking gather of whatever
				// is already queued (upstream GROUP_COMMIT_LOOP).
			GROUP_COMMIT_LOOP:
				for i := 0; i < r.config().MaxAppendEntries; i++ {
					select {
					case newLog := <-r.applyCh:
						ready = append(ready, newLog)
					default:
						break GROUP_COMMIT_LOOP
					}
				}
			}

			// Dispatch the logs
			if stepDown {
				// we're in the process of stepping down as leader, don't process anything new
				for i := range ready {
					ready[i].respond(ErrNotLeader)
				}
			} else {
				r.dispatchLogs(ready)
			}

		case <-lease:
			r.mainThreadSaturation.working()
			// Check if we've exceeded the lease, potentially stepping down
			maxDiff := r.checkLeaderLease()

			// Next check interval should adjust for the last node we've
			// contacted, without going negative
			checkInterval := r.config().LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}

			// Renew the lease timer
			lease = time.After(checkInterval) // 下次检查间隔按最慢的法定成员校准, 但不低于 minCheckInterval

		case <-r.leaderNotifyCh:
			for _, repl := range r.leaderState.replState {
				asyncNotifyCh(repl.notifyCh)
			}

		case <-r.followerNotifyCh:
			//  Ignore since we are not a follower

		case <-r.shutdownCh:
			return
		}
	}
}

// verifyLeader 发起一次 leader 校验: 自投一票并触发所有 follower 立即心跳, 由主线程调用。
// verifyLeader must be called from the main thread for safety.
// Causes the followers to attempt an immediate heartbeat.
func (r *Raft) verifyLeader(v *verifyFuture) {
	// Current leader always votes for self
	v.votes = 1

	// Set the quorum size, hot-path for single node
	v.quorumSize = r.quorumSize()
	if v.quorumSize == 1 {
		v.respond(nil)
		return
	}

	// Track this request
	v.notifyCh = r.verifyCh
	r.leaderState.notify[v] = struct{}{}

	// Trigger immediate heartbeats
	for _, repl := range r.leaderState.replState {
		repl.notifyLock.Lock()
		repl.notify[v] = struct{}{}
		repl.notifyLock.Unlock()
		asyncNotifyCh(repl.notifyCh)
	}
}

// leadershipTransfer 是领导权转移的主体: 先等目标 follower 追平到 leader 的 lastIndex, 再向它发 TimeoutNow 令其立刻竞选。
// leadershipTransfer is doing the heavy lifting for the leadership transfer.
//
// 算法解读：
//
//	原理：转移分两步 —— 先循环等待目标 follower 的 nextIndex 超过 leader 的 lastIndex(即已追平), 每轮通过
//	triggerDeferErrorCh 催一次复制并等它的结果; 追平后再发一条 TimeoutNow, 让目标立刻超时竞选。
//	关键不变量：必须「先追平再让位」—— 否则目标落后于 leader, 当选后要么丢数据要么被迫追日志; 整个过程受
//	stopCh 控制, 一旦离开 leaderLoop(丢了领导权)就中止, 此时返回 nil 表示「以预期方式结束」。
//	代价与边界：等待追平没有内置超时, 超时由 leaderLoop 外的看门 goroutine(ElectionTimeout)负责, 超时后关
//	掉 stopCh 并回错误; TimeoutNow 是阻塞调用且无法中止, 所以它被放在 goroutine 里跑, 保证 leaderLoop 不被
//	卡住。
//	设计权衡：把「追平」与「发 TimeoutNow」分成两步而非直接让位, 是为了让转移在有未复制日志时也安全; 代价
//	是转移耗时取决于落后 follower 的追平速度。
func (r *Raft) leadershipTransfer(id ServerID, address ServerAddress, repl *followerReplication, stopCh chan struct{}, doneCh chan error) {
	// make sure we are not already stopped
	select {
	case <-stopCh:
		doneCh <- nil
		return
	default:
	}

	for atomic.LoadUint64(&repl.nextIndex) <= r.getLastIndex() {
		err := &deferError{}
		err.init()
		repl.triggerDeferErrorCh <- err
		select {
		case err := <-err.errCh:
			if err != nil {
				doneCh <- err
				return
			}
		case <-stopCh:
			doneCh <- nil
			return
		}
	}

	// Step ?: the thesis describes in chap 6.4.1: Using clocks to reduce
	// messaging for read-only queries. If this is implemented, the lease
	// has to be reset as well, in case leadership is transferred. This
	// implementation also has a lease, but it serves another purpose and
	// doesn't need to be reset. The lease mechanism in our raft lib, is
	// setup in a similar way to the one in the thesis, but in practice
	// it's a timer that just tells the leader how often to check
	// heartbeats are still coming in.

	// Step 3: send TimeoutNow message to target server.
	err := r.trans.TimeoutNow(id, address, &TimeoutNowRequest{RPCHeader: r.getRPCHeader()}, &TimeoutNowResponse{})
	if err != nil {
		err = fmt.Errorf("failed to make TimeoutNow RPC to %v: %v", id, err)
	}
	doneCh <- err
}

// checkLeaderLease 检查最近一个租约窗口内能否联系到法定人数, 联系不上就下台为 follower; 返回最长的「无联系」时长, 只能在主线程调用。
// checkLeaderLease is used to check if we can contact a quorum of nodes
// within the last leader lease interval. If not, we need to step down,
// as we may have lost connectivity. Returns the maximum duration without
// contact. This must only be called from the main thread.
//
// 算法解读：
//
//	原理：遍历最新配置里的 voter, 统计在 LeaderLeaseTimeout 内有过联系的个数(自己永远算一个); 够法定人数就
//	把租约截止时间发布为 now - maxDiff + LeaderLeaseTimeout, 不够就下台为 follower。
//	关键不变量：租约只能由「刚确认过的法定联系」赢得, 不能从上一任期继承(runLeader 里把截止时间清零);
//	下台时同样清零, 否则失去领导权的节点还能继续按租约读。
//	代价与边界：maxDiff 是「最慢的那个法定成员有多久没联系」, 用它做截止时间等于按最坏情况打折, 保守但
//	安全; 下一次检查间隔 = LeaderLeaseTimeout - maxDiff, 且不低于 minCheckInterval(10ms), 避免空转。
//	设计权衡：租约让 leader 可本地服务线性一致读而不必每读一轮往返; 代价是读的时效上界依赖时钟, 只在不要求
//	严格线性一致的档位下才可放开。
func (r *Raft) checkLeaderLease() time.Duration {
	// Track contacted nodes, we can always contact ourself
	contacted := 0

	// Store lease timeout for this one check invocation as we need to refer to it
	// in the loop and would be confusing if it ever becomes reloadable and
	// changes between iterations below.
	leaseTimeout := r.config().LeaderLeaseTimeout

	// Check each follower
	var maxDiff time.Duration
	now := time.Now()
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				contacted++
				continue
			}
			f := r.leaderState.replState[server.ID]
			diff := now.Sub(f.LastContact())
			if diff <= leaseTimeout {
				contacted++
				if diff > maxDiff {
					maxDiff = diff
				}
			} else {
				// Log at least once at high value, then debug. Otherwise it gets very verbose.
				if diff <= 3*leaseTimeout {
					r.logger.Warn("failed to contact", "server-id", server.ID, "time", diff)
				} else {
					r.logger.Debug("failed to contact", "server-id", server.ID, "time", diff)
				}
			}
			metrics.AddSample([]string{"raft", "leader", "lastContact"}, float32(diff/time.Millisecond))
		}
	}

	// Verify we can contact a quorum
	quorum := r.quorumSize()
	if contacted >= quorum {
		// Renew the lease: the most recent quorum contact happened maxDiff ago,
		// so the deadline is now - maxDiff + LeaderLeaseTimeout. Publishing this
		// atomically lets the lease-read path (leaderLeaseValid) decide validity
		// from any goroutine without touching main-thread state.
		expiry := now.Add(leaseTimeout - maxDiff)
		r.leaderLeaseExpiry.Store(expiry.UnixNano())
	} else {
		r.logger.Warn("failed to contact quorum of nodes, stepping down")
		r.setState(Follower)
		metrics.IncrCounter([]string{"raft", "transition", "leader_lease_timeout"}, 1)
	}
	return maxDiff
}

// leaderLeaseValid 报告本节点当前是否持有有效领导权租约(仍是 leader, 且主线程在 LeaderLeaseTimeout 内确认过法定联系); 可从任意 goroutine 调用, 只读原子截止时间, 绝不主动下台。
// leaderLeaseValid reports whether this node currently holds a valid leadership
// lease: it is the leader AND the main thread has confirmed contact with a
// quorum of voters within LeaderLeaseTimeout. Unlike checkLeaderLease it never
// steps down and may be called from any goroutine — it reads only the atomic
// lease deadline published by checkLeaderLease plus thread-safe accessors, and
// derives the quorum size from the snapshot of the latest configuration rather
// than the main-thread copy. A zero deadline (leader not yet contacted a quorum,
// or not the leader) reports invalid.
//
// 算法解读：
//
//	原理：读侧只做两件事 —— 确认自己仍是 leader, 再读 checkLeaderLease 发布的原子截止时间, 未过期即有效;
//	完全不碰主线程状态, 因此可从任意 goroutine 调用。
//	关键不变量：截止时间为 0 表示「尚未确认过法定联系」(或已下台), 一律判无效 —— 把 0 当作「永不过期」会让
//	刚当选的 leader 在还没联系上任何人时就服务读。
//	代价与边界：这是租约读而非严格线性一致读: 时钟漂移或长 GC 停顿可能让读在真实 leader 已换届后仍短暂
//	成立。
//	设计权衡：把判定拆成「主线程发布 + 任意线程只读原子值」, 避免读路径去抢主线程的锁; 代价是读的判定与
//	主线程状态之间多一层间接。
func (r *Raft) leaderLeaseValid() bool {
	if r.State() != Leader {
		return false
	}
	deadline := r.leaderLeaseExpiry.Load()
	return deadline != 0 && time.Now().UnixNano() < deadline
}

// quorumSize 返回当前最新配置下法定人数的大小(投票成员数过半加一), 只能在主线程调用。
// quorumSize is used to return the quorum size. This must only be called on
// the main thread.
// TODO: revisit usage
func (r *Raft) quorumSize() int {
	voters := 0
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}

// restoreUserSnapshot 手动消费一份外部快照(如从备份恢复): 用当前配置而非快照内配置, 并刻意在 lastIndex 上烧出一个空洞, 迫使后续复制走快照路径; 只能在 leader 上运行。
// restoreUserSnapshot is used to manually consume an external snapshot, such
// as if restoring from a backup. We will use the current Raft configuration,
// not the one from the snapshot, so that we can restore into a new cluster. We
// will also use the higher of the index of the snapshot, or the current index,
// and then add 1 to that, so we force a new state with a hole in the Raft log,
// so that the snapshot will be sent to followers and used for any new joiners.
// This can only be run on the leader, and returns a future that can be used to
// block until complete.
//
// 算法解读：
//
//	原理：用当前(而非快照内)的配置创建一份新快照, 索引取 max(快照 index, 当前 lastIndex)+1 —— 即在日志里
//	刻意烧出一个空洞; 随后把该状态写进 lastLog/lastApplied/lastSnapshot, 并清掉旧日志。
//	关键不变量：索引必须严格大于已有 lastIndex 且无对应日志条目 —— follower 复制到该索引时因查不到日志而
//	触发 InstallSnapshot, 新节点也能直接靠快照加入; 配置必须已提交(committedIndex == latestIndex)才允许恢复,
//	因为快照格式无法表达「未提交的配置变更」。
//	代价与边界：恢复会先中止所有 inflight 请求(回 ErrAbortedByRestore); FSM 恢复失败直接 panic —— 此时进程
//	状态已不可信。
//	设计权衡：宁可烧一个索引空洞也不用连续索引, 是为了让「快照 + 日志」两套来源不产生歧义; 代价是日志里
//	永远留一个洞, 必须靠 MonotonicLogStore 的 removeOldLogs 而非 compact 来清理。
func (r *Raft) restoreUserSnapshot(meta *SnapshotMeta, reader io.Reader) error {
	defer metrics.MeasureSince([]string{"raft", "restoreUserSnapshot"}, time.Now())

	// Sanity check the version.
	version := meta.Version
	if version < SnapshotVersionMin || version > SnapshotVersionMax {
		return fmt.Errorf("unsupported snapshot version %d", version)
	}

	// We don't support snapshots while there's a config change
	// outstanding since the snapshot doesn't have a means to
	// represent this state.
	committedIndex := r.configurations.committedIndex
	latestIndex := r.configurations.latestIndex
	if committedIndex != latestIndex {
		return fmt.Errorf("cannot restore snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			latestIndex, committedIndex)
	}

	// Cancel any inflight requests.
	for {
		e := r.leaderState.inflight.Front()
		if e == nil {
			break
		}
		e.Value.(*logFuture).respond(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}

	// We will overwrite the snapshot metadata with the current term,
	// an index that's greater than the current index, or the last
	// index in the snapshot. It's important that we leave a hole in
	// the index so we know there's nothing in the Raft log there and
	// replication will fault and send the snapshot.
	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	if meta.Index > lastIndex {
		lastIndex = meta.Index
	}
	lastIndex++

	// Dump the snapshot. Note that we use the latest configuration,
	// not the one that came with the snapshot.
	sink, err := r.snapshots.Create(version, lastIndex, term,
		r.configurations.latest, r.configurations.latestIndex, r.trans)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	n, err := io.Copy(sink, reader)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %v", err)
	}
	if n != meta.Size {
		_ = sink.Cancel()
		return fmt.Errorf("failed to write snapshot, size didn't match (%d != %d)", n, meta.Size)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %v", err)
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// Restore the snapshot into the FSM. If this fails we are in a
	// bad state so we panic to take ourselves out.
	fsm := &restoreFuture{ID: sink.ID()}
	fsm.ShutdownCh = r.shutdownCh
	fsm.init()
	select {
	case r.fsmMutateCh <- fsm:
	case <-r.shutdownCh:
		return ErrRaftShutdown
	}
	if err := fsm.Error(); err != nil {
		panic(fmt.Errorf("failed to restore snapshot: %v", err))
	}

	// We set the last log so it looks like we've stored the empty
	// index we burned. The last applied is set because we made the
	// FSM take the snapshot state, and we store the last snapshot
	// in the stable store since we created a snapshot as part of
	// this process.
	r.setLastLog(lastIndex, term)
	r.setLastApplied(lastIndex)
	r.setLastSnapshot(lastIndex, term)

	// Remove old logs if r.logs is a MonotonicLogStore. Log any errors and continue.
	if logs, ok := r.logs.(MonotonicLogStore); ok && logs.IsMonotonic() {
		if err := r.removeOldLogs(); err != nil {
			r.logger.Error("failed to remove old logs", "error", err)
		}
	}

	r.logger.Info("restored user snapshot", "index", lastIndex)
	return nil
}

// appendConfigurationEntry 把一次成员变更落成配置日志条目并派发, 同时更新 latest 配置、commitment 与复制管线; 只能在主线程调用。
// appendConfigurationEntry changes the configuration and adds a new
// configuration entry to the log. This must only be called from the
// main thread.
func (r *Raft) appendConfigurationEntry(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, r.configurations.latestIndex, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.logger.Info("updating configuration",
		"command", future.req.command,
		"server-id", future.req.serverID,
		"server-addr", future.req.serverAddress,
		"servers", hclog.Fmt("%+v", configuration.Servers))

	// In pre-ID compatibility mode we translate all configuration changes
	// in to an old remove peer message, which can handle all supported
	// cases for peer changes in the pre-ID world (adding and removing
	// voters). Both add peer and remove peer log entries are handled
	// similarly on old Raft servers, but remove peer does extra checks to
	// see if a leader needs to step down. Since they both assert the full
	// configuration, then we can safely call remove peer for everything.
	if r.protocolVersion < 2 {
		future.log = Log{
			Type: LogRemovePeerDeprecated,
			Data: encodePeers(configuration, r.trans),
		}
	} else {
		future.log = Log{
			Type: LogConfiguration,
			Data: EncodeConfiguration(configuration),
		}
	}

	r.dispatchLogs([]*logFuture{&future.logFuture})
	index := future.Index()
	r.setLatestConfiguration(configuration, index)
	r.leaderState.commitment.setConfiguration(configuration)
	r.startStopReplication()
}

// dispatchLogs 由 leader 调用: 给一批 logFuture 分配递增索引、置为 inflight, 落盘(或交给异步写者)后通知复制管线开始复制。
// dispatchLog is called on the leader to push a log to disk, mark it
// as inflight and begin replication of it.
//
// 算法解读：
//
//	原理：给批次里每条 logFuture 依次分配 lastIndex+1、+2 …, 置 Term/AppendedAt, 压入 inflight 队列; 然后
//	同步模式直接 StoreLogs + fsync 后 commitment.match(self), 异步模式把批次塞进 pending 缓冲并投给
//	asyncWriteCh, 立即通知复制管线, 让 follower 复制与 leader 自身 fsync 重叠。
//	关键不变量：索引分配必须越过 pending 尾巴 —— 异步模式下已派发未落盘的条目还在 pending 缓冲里, 若只按
//	持久化 lastLog 分配, 两个在写者追上之前先后派发的批次会拿到相同索引, 后一批静默覆盖前一批; commitIndex
//	只能由 leader 自身已持久化的日志推进(persistAsyncBatch 里的 commitment.match), 绝不能被内存 pending 推进。
//	代价与边界：异步模式让 follower 能拉取 leader 尚未 fsync 的条目(靠 pending 缓冲的 getLog 供给), 因此
//	leader 崩溃可能丢掉「已复制给 follower 但自己没落盘」的条目 —— 仍满足 Raft 安全性(未持久化的不会被
//	提交), 但表现为回滚。
//	设计权衡：把 fsync 移出主线程, 让 leader 写盘与网络复制并行, 单批延迟从 fsync+往返降为
//	max(fsync, 往返); 代价是引入 pending 这一层「未持久化但可见」的状态, 必须靠「索引越过尾巴」与「commit
//	封顶」两条规则兜住。
func (r *Raft) dispatchLogs(applyLogs []*logFuture) {
	now := time.Now()
	defer metrics.MeasureSince([]string{"raft", "leader", "dispatchLog"}, now)

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()

	// vraft async leader persist: with the writer persisting asynchronously, the
	// durable last index lags behind the dispatched entries still queued in the
	// pending buffer. Index assignment must account for that pending tail,
	// otherwise two overlapping batches dispatched before the writer catches up
	// would be assigned the same index (and the later entries would silently
	// overwrite the earlier ones on every follower). Without async persist the
	// pending buffer is always empty and lastIndex is unchanged.
	if r.config().AsyncLeaderPersist {
		r.pendingMu.Lock()
		if len(r.pendingLogs) > 0 {
			if top := r.pendingBase + uint64(len(r.pendingLogs)) - 1; top > lastIndex {
				lastIndex = top
			}
		}
		r.pendingMu.Unlock()

		// ★游标窗口兜底(2026-10-24): 异步写者的推进顺序是 StoreLogs → setLastLog
		// → dropPendingThrough。在 StoreLogs 已返回、setLastLog 未执行之间, store
		// 的 last 已经是 N 而内存 lastLog 仍是 N-1; 此时若 pending 也不再覆盖 N
		// (下台清理 clearPending 会清掉在途批的尾巴; 或者 getLastIndex 与上面
		// pendingMu 两次取锁之间写者恰好完成 setLastLog + dropPendingThrough),
		// 只按 lastLog/pending 分配索引就会把已落盘的 N 再派发一次 —— StoreLogs
		// 收到 batch first == store last 的重叠批(现场 leader 异步持久化
		// "store last=15007, batch first=15007" 即此)。这里以 store 的真实
		// LastIndex() 兜底, 保证新批永远从真实最后一条之后开始。
		// 只取更大值: store 落后于 lastLog(快照装回/尚未落盘)时保持原游标。
		if storeLast, err := r.logs.LastIndex(); err == nil && storeLast > lastIndex {
			lastIndex = storeLast
		}
	}

	n := len(applyLogs)
	logs := make([]*Log, n)
	metrics.SetGauge([]string{"raft", "leader", "dispatchNumLogs"}, float32(n))
	metrics.AddSample([]string{"raft", "leader", "dispatchBatchSize"}, float32(n))

	for idx, applyLog := range applyLogs {
		applyLog.dispatch = now
		lastIndex++
		applyLog.log.Index = lastIndex
		applyLog.log.Term = term
		applyLog.log.AppendedAt = now
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)
	}

	commitIndex := r.getCommitIndex()
	r.tryStageCommitIndex(commitIndex)

	if r.config().AsyncLeaderPersist {
		// vraft async leader persist (12.1.3): notify replicators immediately
		// and queue the batch for the (serial) async writer, so follower
		// replication overlaps the leader's own fsync. The writer persists
		// batches in order; the futures stay on the inflight list and are
		// responded once they are committed (which can only happen after the
		// batch is durable on the leader).
		r.appendPending(logs, lastIndex-uint64(n)+1) // 批次起始索引 = 末索引 - 条数 + 1
		for _, f := range r.leaderState.replState {
			asyncNotifyCh(f.triggerCh)
		}
		select {
		case r.asyncWriteCh <- asyncWrite{
			logs:       logs,
			futures:    applyLogs,
			lastIndex:  lastIndex,
			term:       term,
			start:      time.Now(),
			commitment: r.leaderState.commitment,
		}:
		case <-r.shutdownCh:
		}
		return
	}

	// Write the log entry locally. MeasureSince captures the store write + fsync
	// latency (the log.store latency metric); with a file-backed LogStore this is
	// the dominant component of a batch's persistence cost.
	storeStart := time.Now()
	if err := r.logs.StoreLogs(logs); err != nil {
		metrics.MeasureSince([]string{"raft", "leader", "logStore", "error"}, storeStart)
		r.logger.Error("failed to commit logs", "error", err)
		for _, applyLog := range applyLogs {
			applyLog.respond(err)
		}
		r.setState(Follower)
		return
	}
	metrics.MeasureSince([]string{"raft", "leader", "logStore"}, storeStart)
	r.leaderState.commitment.match(r.localID, lastIndex)

	// Update the last log since it's on disk now
	r.setLastLog(lastIndex, term)

	// Notify the replicators of the new log
	for _, f := range r.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}
}

// appendPending 把一批已派发日志记入异步持久化的 pending 缓冲; 必须在主线程按索引升序调用。
// appendPending records a dispatched batch in the async-persist pending buffer.
// Must be called from the main thread (dispatchLogs), in index order.
func (r *Raft) appendPending(logs []*Log, base uint64) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if len(r.pendingLogs) == 0 {
		r.pendingBase = base
	}
	r.pendingLogs = append(r.pendingLogs, logs...)
}

// dropPendingThrough 在异步写者持久化完成后, 从 pending 缓冲中丢弃 <= index 的前缀。
// dropPendingThrough removes entries up to and including index from the
// pending buffer once they have been persisted by the async writer.
//
// 算法解读：
//
//	原理：pending 缓冲是一段连续索引 [pendingBase, pendingBase+len-1]。写者持久化到 index 后丢掉这段前缀:
//	若 index 已覆盖到顶端则整体清空, 否则把剩余条目 copy 到切片头部并前移 pendingBase。
//	关键不变量：pendingBase 与 len(pendingLogs) 必须始终满足 top = pendingBase+len-1, 任何一步错位都会让
//	getLog 与 getReplicationLastIndex 读到错误的索引; index 落在缓冲之前(已丢过)时直接返回。
//	代价与边界：用 copy 前移而非环形缓冲, 每次丢弃都要搬移剩余元素 —— 缓冲长度受在途批次数限制, 实际很小。
//	设计权衡：选简单切片 + copy 而非 ring buffer, 是为了让 getLog 的随机索引查找保持 O(1) 且代码可读; 代价
//	是丢弃时的一次 O(剩余长度) 搬移。
func (r *Raft) dropPendingThrough(index uint64) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if len(r.pendingLogs) == 0 {
		return
	}
	top := r.pendingBase + uint64(len(r.pendingLogs)) - 1
	if index >= top {
		r.pendingBase = 0
		r.pendingLogs = r.pendingLogs[:0]
		return
	}
	if index < r.pendingBase {
		return
	}
	drop := int(index-r.pendingBase) + 1
	copy(r.pendingLogs, r.pendingLogs[drop:])
	r.pendingLogs = r.pendingLogs[:len(r.pendingLogs)-drop]
	r.pendingBase += uint64(drop)
}

// clearPending 清空整个 pending 缓冲; 领导权丢失时调用, 避免未来 leader 复用同一段索引时读到陈旧条目。
// clearPending drops the whole pending buffer. Called on leadership loss so a
// future leader never serves stale entries for indexes it re-issues.
func (r *Raft) clearPending() {
	r.pendingMu.Lock()
	r.pendingBase = 0
	r.pendingLogs = r.pendingLogs[:0]
	r.pendingMu.Unlock()
}

// getLog 读取一条日志: 优先命中异步持久化的 pending 缓冲(已派发但尚未落盘), 未命中再回落到持久化 LogStore。
// getLog reads a log entry, preferring the async-persist pending buffer (for
// entries dispatched but not yet durable) and falling back to the durable log
// store.
func (r *Raft) getLog(index uint64, log *Log) error {
	r.pendingMu.Lock()
	if r.pendingBase > 0 && index >= r.pendingBase { // pendingBase == 0 表示缓冲空(合法日志索引从 1 起, 0 不可能是索引)
		if off := int(index - r.pendingBase); off < len(r.pendingLogs) {
			*log = *r.pendingLogs[off]
			r.pendingMu.Unlock()
			return nil
		}
	}
	r.pendingMu.Unlock()
	return r.logs.GetLog(index, log)
}

// getReplicationLastIndex 返回 leader 当前可复制给 follower 的最高索引: 持久化的 lastLog 再加上 pending 尾巴(已派发未 fsync 的条目)。
// getReplicationLastIndex returns the highest index the leader may currently
// replicate to followers: the durable last log plus the async-persist pending
// tail (entries already dispatched but not yet fsynced). Without async persist
// the pending buffer is always empty, so this returns the durable index.
//
// 算法解读：
//
//	原理：复制水位取「持久化 lastLog」与「pending 缓冲顶端」的较大者。异步模式下 pending 里的条目虽未
//	fsync, 但内容已在 leader 内存且完整, 可以安全地复制给 follower(follower 落盘后它就持久了)。
//	关键不变量：水位只可能 >= 持久化 lastLog, 不会倒退; 未开异步持久化时 pending 恒空, 退化为返回持久化
//	lastLog, 与上游行为一致。
//	代价与边界：leader 把 pending 条目复制出去之后崩溃, 自己会丢掉这些条目而 follower 保留 —— 这不违反
//	安全性(它们尚未提交), 但会在新 leader 上任后被覆盖。
//	设计权衡：让 follower 能拉取未持久化条目, 是为了不让 leader 的 fsync 成为复制吞吐的瓶颈; 安全性由
//	「commitIndex 被 leader 自身持久化进度封顶」兜底。
func (r *Raft) getReplicationLastIndex() uint64 {
	lastLogIdx, _ := r.getLastLog()
	if !r.config().AsyncLeaderPersist {
		return lastLogIdx
	}
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if len(r.pendingLogs) == 0 {
		return lastLogIdx
	}
	top := r.pendingBase + uint64(len(r.pendingLogs)) - 1
	if top > lastLogIdx {
		return top
	}
	return lastLogIdx
}

// asyncCoalesceMax is the number of queued asyncWrite batches the writer will
// coalesce into a single StoreLogs call. Bounded so a sustained producer cannot
// grow the merged batch without bound (and so the writer keeps returning to the
// select, where shutdown is observed). Values are balanced against MaxAppendEntries:
// coalescing up to MaxAppendEntries writes restores the batch accumulation that
// sync persist gets for free from blocking on the fsync.
const asyncCoalesceMax = 64

// runAsyncWriter 是每任期的异步持久化协程: 按派发顺序串行地把 leader 批次写盘, 使 store 与 raft lastLog 单调推进, 同时 follower 复制并行进行。
// runAsyncWriter is the per-leadership goroutine that persists dispatched
// leader batches serially, in dispatch order, so the store and the raft
// last-log index advance monotonically even while the batches are replicated
// to followers in parallel.
//
// 算法解读：
//
//	原理：写者从 asyncWriteCh 取一个批次后, 立刻以非阻塞方式再吸最多 asyncCoalesceMax(64) 个已排队批次,
//	合并成一次 StoreLogs 调用 —— 同步模式靠阻塞在 fsync 上自然攒批, 异步模式没有这个阻塞, 必须显式合并。
//	关键不变量：asyncWriteCh 是 FIFO 且属于同一任期, 合并保持派发顺序与 term; 合并只取「立即可得」的批次,
//	绝不等待, 因此写者总能回到 select 观察 stopCh/shutdownCh。
//	代价与边界：上限 64 使持续生产者不能把单次 StoreLogs 撑到无界(内存与单次 fsync 时长都有上界), 并与
//	MaxAppendEntries 对齐, 使异步模式的批大小与同步模式相当。
//	设计权衡：不设上限会在高负载下把写者变成「永远在攒批、从不落盘」; 设成 1 则退回每条一次 fsync。64 是
//	「恢复同步模式攒批效果」与「保持响应关机」之间的折中。
func (r *Raft) runAsyncWriter(stopCh chan struct{}) {
	for {
		select {
		case w := <-r.asyncWriteCh:
			// vraft: coalesce as many queued writes as are immediately available
			// into one persistence batch. In sync mode the leader loop blocks on
			// the store fsync, and that stall naturally accumulates entries into
			// large batches. Async persist removes the stall, so the loop drains
			// the apply channel fast and dispatches single-entry batches; without
			// coalescing the writer would pay one fsync round (~10-40ms on
			// typical storage) per entry. Merging restores one-fsync-per-group.
			// Writes queued on asyncWriteCh are FIFO and belong to the same
			// leadership, so merging preserves dispatch order and term.
		drain:
			for i := 0; i < asyncCoalesceMax; i++ {
				select {
				case more := <-r.asyncWriteCh:
					w.logs = append(w.logs, more.logs...)
					w.futures = append(w.futures, more.futures...)
					if more.lastIndex > w.lastIndex {
						w.lastIndex = more.lastIndex
					}
					if more.start.Before(w.start) {
						w.start = more.start
					}
				case <-stopCh:
					break drain
				case <-r.shutdownCh:
					break drain
				default:
					break drain
				}
			}
			r.persistAsyncBatch(w)
		case <-stopCh:
			return
		case <-r.shutdownCh:
			return
		}
	}
}

// persistAsyncBatch 持久化一个 leader 批次, 并在批次落盘后推进 commitment。
// persistAsyncBatch persists one leader batch and advances commitment once the
// batch is durable. See the asyncWrite comment for the safety rules.
//
// 算法解读：
//
//	原理：异步写者串行地把批次 StoreLogs 落盘, 成功后 setLastLog 并调用 commitment.match(self, lastIndex)
//	—— 即用「leader 自己已持久化的最高索引」给 commitIndex 封顶。
//	关键不变量：只有仍处于同一任期、且 commitment 仍是当前那个时, 才计入提交; 写失败时把 asyncWriteFailed
//	置真、唤醒主线程下台, 并丢弃 pending 到该索引, 此后不再接受新提交 —— 因为已无法保证日志的持久性。
//	代价与边界：commit 因此被「leader 自身 fsync 进度」限速, 即使多数 follower 早已确认; 这是刻意的: 提交
//	一条 leader 自己都可能丢的日志会破坏 Raft 安全性。
//	设计权衡：不用「多数派确认即提交」的乐观做法, 是因为异步持久化下多数派确认的是 leader 内存里的条目,
//	leader 崩溃即丢 —— 必须先自持久化再认提交。
func (r *Raft) persistAsyncBatch(w asyncWrite) {
	if r.asyncWriteFailed.Load() {
		for _, f := range w.futures {
			f.respond(r.asyncStoreErr())
		}
		r.dropPendingThrough(w.lastIndex)
		return
	}
	// 12.14.8 #19 取证: 拆"排队+合并等待"(既有 logStore 自 w.start=dispatch 起算)与
	// "纯写盘"(logStoreWrite 紧贴 StoreLogs) —— lurch 期间若前者膨胀而后者平坦,
	// 卡点在 async 写者串行队列; 同步膨胀则卡点在盘。
	writeStart := time.Now()
	err := r.logs.StoreLogs(w.logs)
	metrics.MeasureSince([]string{"raft", "leader", "logStoreWrite"}, writeStart)
	if err != nil {
		metrics.MeasureSince([]string{"raft", "leader", "logStore", "error"}, w.start)
		r.logger.Error("failed to commit logs asynchronously", "error", err)
		r.asyncWriteFailed.Store(true)
		r.asyncStoreErrStore(err)
		for _, f := range w.futures {
			f.respond(err)
		}
		r.dropPendingThrough(w.lastIndex)
		// Ask the leader loop to step down; we can no longer guarantee
		// durability of the log.
		select {
		case r.asyncPersistErrCh <- struct{}{}:
		default:
		}
		return
	}
	metrics.MeasureSince([]string{"raft", "leader", "logStore"}, w.start)
	metrics.AddSample([]string{"raft", "leader", "asyncPersistBatchSize"}, float32(len(w.logs)))
	r.setLastLog(w.lastIndex, w.term)
	r.dropPendingThrough(w.lastIndex)
	// Count this batch toward commitment only while we are still the leader of
	// the same term (i.e., this commitment object is current). The commitment's
	// self-cap keeps the commit index from exceeding this durable last log.
	if r.getState() == Leader && r.getCurrentTerm() == w.term {
		w.commitment.match(r.localID, w.lastIndex)
	}
}

// processLogs 把 lastApplied 之后、到 index 为止的日志按 MaxAppendEntries 分批喂给 FSM, 并把 lastApplied 推进到 index; leader 与 follower 都会调用(follower 传 futures=nil)。
// applied up to the given index limit.
// This can be called from both leaders and followers.
// Followers call this from AppendEntries, for n entries at a time, and always
// pass futures=nil.
// Leaders call this when entries are committed. They pass the futures from any
// inflight logs.
//
// 算法解读：
//
//	原理：从 lastApplied+1 逐条走到 index; leader 侧优先用 futures 里的内存条目(避免回读日志文件), follower
//	侧从 LogStore 读。需要应用的条目攒够 MaxAppendEntries 就投一次 fsmMutateCh, 最后把 lastApplied 推进到
//	index。
//	关键不变量：lastApplied 只增不减, 且必须在所有批次投递之后才推进 —— 提前推进会让崩溃后的重放漏掉尚未
//	真正应用的条目; 已应用过的 index(<= lastApplied)直接拒绝。
//	代价与边界：单次调用的最大批次内存 = MaxAppendEntries 个 commitTuple; 若 FSM 投递通道阻塞在关机, 未投
//	递的 future 会被回以 ErrRaftShutdown。
//	设计权衡：批量投递而非逐条投递, 摊薄 FSM 的通道/唤醒开销; 但批内不做「读到一半失败」的补偿 —— 读日志
//	失败直接 panic, 因为此时状态已不可信。
func (r *Raft) processLogs(index uint64, futures map[uint64]*logFuture) {
	// Reject logs we've applied already
	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.logger.Warn("skipping application of old log", "index", index)
		return
	}

	applyBatch := func(batch []*commitTuple) {
		select {
		case r.fsmMutateCh <- batch:
		case <-r.shutdownCh:
			for _, cl := range batch {
				if cl.future != nil {
					cl.future.respond(ErrRaftShutdown)
				}
			}
		}
	}

	// Store maxAppendEntries for this call in case it ever becomes reloadable. We
	// need to use the same value for all lines here to get the expected result.
	maxAppendEntries := r.config().MaxAppendEntries

	batch := make([]*commitTuple, 0, maxAppendEntries)

	// Apply all the preceding logs
	for idx := lastApplied + 1; idx <= index; idx++ {
		var preparedLog *commitTuple
		// Get the log, either from the future or from our log store
		future, futureOk := futures[idx]
		if futureOk {
			preparedLog = r.prepareLog(&future.log, future)
		} else {
			l := new(Log)
			if err := r.logs.GetLog(idx, l); err != nil {
				r.logger.Error("failed to get log", "index", idx, "error", err)
				panic(err)
			}
			preparedLog = r.prepareLog(l, nil)
		}

		switch {
		case preparedLog != nil:
			// If we have a log ready to send to the FSM add it to the batch.
			// The FSM thread will respond to the future.
			batch = append(batch, preparedLog)

			// If we have filled up a batch, send it to the FSM
			if len(batch) >= maxAppendEntries {
				applyBatch(batch)
				batch = make([]*commitTuple, 0, maxAppendEntries)
			}

		case futureOk:
			// Invoke the future if given.
			future.respond(nil)
		}
	}

	// If there are any remaining logs in the batch apply them
	if len(batch) != 0 {
		applyBatch(batch)
	}

	// Update the lastApplied index and term
	r.setLastApplied(index)
}

// prepareLog 决定一条已提交日志是否需要送进 FSM: 命令/屏障/新版配置条目需要, noop 与旧版 peer 变更条目忽略。
// processLog is invoked to process the application of a single committed log entry.
func (r *Raft) prepareLog(l *Log, future *logFuture) *commitTuple {
	switch l.Type {
	case LogBarrier:
		// Barrier is handled by the FSM
		fallthrough

	case LogCommand:
		return &commitTuple{l, future}

	case LogConfiguration:
		// Only support this with the v2 configuration format
		if r.protocolVersion > 2 {
			return &commitTuple{l, future}
		}
	case LogAddPeerDeprecated:
	case LogRemovePeerDeprecated:
	case LogNoop:
		// Ignore the no-op

	default:
		panic(fmt.Errorf("unrecognized log type: %#v", l))
	}

	return nil
}

// tryStageCommitIndex 在启用 RestoreCommittedLogs 且 LogStore 实现 CommitTrackingLogStore 时, 把提交索引落进存储; 否则静默返回。
// tryStageCommitIndex updates the commit index in persist store if restore committed logs is enabled and log store implements CommitTrackingLogStore.
func (r *Raft) tryStageCommitIndex(commitIndex uint64) {
	if !r.RestoreCommittedLogs {
		return
	}
	store, ok := r.logs.(CommitTrackingLogStore)
	if !ok {
		return
	}
	if err := store.StageCommitIndex(commitIndex); err != nil {
		r.logger.Error("failed to stage commit index in commit tracking log store", "index", commitIndex, "error", err)
	}
}

// processRPC 按命令类型把入站 RPC 分派给对应处理函数, 未知命令回 "unexpected command" 错误; 只能在主线程调用。
// processRPC is called to handle an incoming RPC request. This must only be
// called from the main thread.
func (r *Raft) processRPC(rpc RPC) {
	if err := r.checkRPCHeader(rpc); err != nil {
		rpc.Respond(nil, err)
		return
	}

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	case *RequestVoteRequest:
		r.requestVote(rpc, cmd)
	case *RequestPreVoteRequest:
		r.requestPreVote(rpc, cmd)
	case *InstallSnapshotRequest:
		r.installSnapshot(rpc, cmd)
	case *TimeoutNowRequest:
		r.timeoutNow(rpc, cmd)
	default:
		r.logger.Error("got unexpected command",
			"command", hclog.Fmt("%#v", rpc.Command))

		rpc.Respond(nil, errors.New(rpcUnexpectedCommandError))
	}
}

// processHeartbeat 是心跳专用的快路径入口, 只接受 AppendEntriesRequest, 供支持该优化的传输层使用; 只能在主线程调用。
// processHeartbeat is a special handler used just for heartbeat requests
// so that they can be fast-pathed if a transport supports it. This must only
// be called from the main thread.
func (r *Raft) processHeartbeat(rpc RPC) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "processHeartbeat"}, time.Now())

	// Check if we are shutdown, just ignore the RPC
	select {
	case <-r.shutdownCh:
		return
	default:
	}

	// Ensure we are only handling a heartbeat
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Error("expected heartbeat, got", "command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// appendEntries 处理 leader 的 AppendEntries RPC: 任期/前缀校验、冲突截断、追加新条目、推进 commitIndex 并把已提交日志喂给 FSM; 只能在主线程调用。
// appendEntries is invoked when we get an append entries RPC call. This must
// only be called from the main thread.
//
// 算法解读：
//
//	原理：先按任期/前缀(PrevLogEntry + PrevLogTerm)校验, 再定位新条目起点: 逐条比对已有条目的 term, 相同则
//	跳过(去重), 遇到 term 不同的位置就 DeleteRange 截断后缀并从这里整段接收。新条目先做 P4 指针解析
//	(resolveElidedEntries), 再 StoreLogs, 最后把 commitIndex 推进到 min(LeaderCommitIndex, 本地 lastIndex),
//	并把由此新提交的条目喂给 FSM。
//	关键不变量：commitIndex 永不超过本地已有日志的 lastIndex(min 保证); 截断后缀后必须同步回退 latest 配置
//	(否则会持有来自被截断条目的配置); 指针解析失败要以 DataMissingFrom 显式回退, 绝不能把 Data 缺失的条目
//	静默落盘。
//	代价与边界：冲突检测逐条回读日志文件, 冲突窗口越大越慢; 前缀不匹配时置 NoRetryBackoff, 让 leader 立刻
//	回退 nextIndex 而不必等退避。
//	设计权衡：先解析指针再落盘(而非先落盘后补 Data), 保证「落盘的条目永远完整」; 代价是解析失败时整批退回,
//	让 leader 对这个 follower 走全量复制。
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, time.Now())
	// Setup a response
	resp := &AppendEntriesResponse{
		RPCHeader:      r.getRPCHeader(),
		Term:           r.getCurrentTerm(),
		LastLog:        r.getLastIndex(),
		Success:        false,
		NoRetryBackoff: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Ignore an older term
	if a.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.getCurrentTerm() || (r.getState() != Follower && !r.candidateFromLeadershipTransfer.Load()) {
		// Ensure transition to follower
		r.setState(Follower)
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
	}

	// Save the current leader
	if len(a.Addr) > 0 {
		r.setLeader(r.trans.DecodePeer(a.Addr), ServerID(a.ID))
	} else {
		r.setLeader(r.trans.DecodePeer(a.Leader), ServerID(a.ID))
	}
	// Verify the last log entry
	if a.PrevLogEntry > 0 { // PrevLogEntry == 0 表示 leader 认为这是第一条日志, 无需前缀校验
		lastIdx, lastTerm := r.getLastEntry()

		var prevLogTerm uint64
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm
		} else {
			var prevLog Log
			if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
				r.logger.Warn("failed to get previous log",
					"previous-index", a.PrevLogEntry,
					"last-index", lastIdx,
					"error", err)
				resp.NoRetryBackoff = true
				return
			}
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			r.logger.Warn("previous log term mis-match",
				"ours", prevLogTerm,
				"remote", a.PrevLogTerm)
			resp.NoRetryBackoff = true
			return
		}
	}

	// Process any new entries
	if len(a.Entries) > 0 {
		start := time.Now()

		// Delete any conflicting entries, skip any duplicates
		lastLogIdx, _ := r.getLastLog()
		// ★同 dispatchLogs 的游标窗口(2026-10-24): 节点刚下台时, 旧任期的异步
		// 写者可能已经 StoreLogs 落盘、但 setLastLog 尚未执行, 且 pending 已被
		// 下台清理清空。若只按内存 lastLog 判定哪些条目"已存在", 会把 store 里
		// 已有的索引当成 newEntries 重写(first == store last)。以 store 的真实
		// last 抬高已存在水位 —— 区间内的条目仍逐条 GetLog 比对 term, term 冲突
		// 照常 DeleteRange 截断, 因此判定语义不变, 只是不再重写已落盘条目。
		// 未开异步持久化时不存在并发写者, 保持原行为(不加这次读取)。
		if r.config().AsyncLeaderPersist {
			if storeLast, err := r.logs.LastIndex(); err == nil && storeLast > lastLogIdx {
				lastLogIdx = storeLast
			}
		}
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {
				newEntries = a.Entries[i:]
				break
			}
			var storeEntry Log
			if err := r.logs.GetLog(entry.Index, &storeEntry); err != nil {
				r.logger.Warn("failed to get log entry",
					"index", entry.Index,
					"error", err)
				return
			}
			if entry.Term != storeEntry.Term {
				r.logger.Warn("clearing log suffix", "from", entry.Index, "to", lastLogIdx)
				if err := r.logs.DeleteRange(entry.Index, lastLogIdx); err != nil {
					r.logger.Error("failed to clear log suffix", "error", err)
					return
				}
				if entry.Index <= r.configurations.latestIndex {
					r.setLatestConfiguration(r.configurations.committed, r.configurations.committedIndex)
				}
				newEntries = a.Entries[i:]
				break
			}
		}

		if n := len(newEntries); n > 0 {
			// Stage the future commit index if possible
			lastNewIndex := newEntries[len(newEntries)-1].Index
			commitIndex := min(a.LeaderCommitIndex, lastNewIndex) // 暂存提交索引只按本批新条目推进, 不超过本批最后一条
			r.tryStageCommitIndex(commitIndex)

			// 12.14 P4: resolve pointer-tier (elided) entries against the local
			// redo file BEFORE storing. Unresolvable → DataMissingFrom, so the
			// leader falls back to full-data replication for this follower.
			// Resolution restores Data in memory; the stored entry is complete
			// again (stores that can't resolve → reject, never silently store a
			// Data-less entry).
			resolveStart := time.Now() // 12.14.8: 指针解析耗时单列(#19 取证: ref 读盘是否挤兑)
			if missIdx, err := resolveElidedEntries(r.logs, newEntries); err != nil {
				r.logger.Warn("cannot resolve elided redo entries locally; asking leader for full data",
					"from_index", missIdx, "error", err)
				resp.DataMissingFrom = missIdx
				resp.NoRetryBackoff = true
				return
			}
			metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "resolveElided"}, resolveStart)

			// Append the new entries
			storeStart := time.Now() // 12.14.8: 纯落盘拆分(既有 storeLogs 口径含冲突检测+解析)
			if err := r.logs.StoreLogs(newEntries); err != nil {
				r.logger.Error("failed to append to logs", "error", err)
				// TODO: leaving r.getLastLog() in the wrong
				// state if there was a truncation above
				return
			}
			metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "storeLogsOnly"}, storeStart)

			// Handle any new configuration changes
			for _, newEntry := range newEntries {
				if err := r.processConfigurationLogEntry(newEntry); err != nil {
					r.logger.Warn("failed to append entry",
						"index", newEntry.Index,
						"error", err)
					rpcErr = err
					return
				}
			}

			// Update the lastLog
			last := newEntries[n-1]
			r.setLastLog(last.Index, last.Term)
		}

		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "storeLogs"}, start)
	}

	// Update the commit index
	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		start := time.Now()
		idx := min(a.LeaderCommitIndex, r.getLastIndex()) // 夹住: 绝不提交本地还没有的日志
		r.setCommitIndex(idx)
		if r.configurations.latestIndex <= idx {
			r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		}
		r.processLogs(idx, nil)
		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "processLogs"}, start)
	}

	// Everything went well, set success
	resp.Success = true
	r.setLastContact()
}

// processConfigurationLogEntry 依据一条配置类日志条目更新 latest 配置(先把原 latest 提升为 committed); 只能在主线程或 NewRaft 起线程前调用。
// processConfigurationLogEntry takes a log entry and updates the latest
// configuration if the entry results in a new configuration. This must only be
// called from the main thread, or from NewRaft() before any threads have begun.
func (r *Raft) processConfigurationLogEntry(entry *Log) error {
	switch entry.Type {
	case LogConfiguration:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)

	case LogAddPeerDeprecated, LogRemovePeerDeprecated:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		conf, err := decodePeers(entry.Data, r.trans)
		if err != nil {
			return err
		}
		r.setLatestConfiguration(conf, entry.Index)
	}
	return nil
}

// requestVote 处理 RequestVote RPC: 依次做配置、已知 leader、任期、是否已投票、日志新旧等校验, 通过后持久化投票并授予。
// requestVote is invoked when we get a request vote RPC call.
//
// 算法解读：
//
//	原理：按序校验 —— 候选人在配置内、没有已知 leader(除非是领导权转移)、任期不旧、自己没在本任期投过票、
//	候选人日志不比自己旧(先比 lastTerm, 相同再比 lastIndex); 全过则持久化投票并授予。
//	关键不变量：同一任期只投一票(靠持久化的 lastVoteTerm/lastVoteCand, 重启后依然生效), 且只投给日志至少
//	一样新的候选人 —— 这两条是选举安全的核心, 破坏任一条都可能选出丢数据的 leader。
//	代价与边界：每次投票都要读 StableStore 并落盘(同步 fsync), 是选举路径上最重的一步; 重复请求同任期同
//	候选人会直接回授予(幂等), 避免网络重传导致候选人收不到票。
//	设计权衡：宁可拒绝也不投「日志更旧」的候选人, 代价是网络分区恢复时旧 leader 更难连任; 这是 Raft 用
//	可用性换一致性的取舍。
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "requestVote"}, time.Now())
	r.observe(*req)

	// Setup a response
	resp := &RequestVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Granted:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Version 0 servers will panic unless the peers is present. It's only
	// used on them to produce a warning message.
	if r.protocolVersion < 2 {
		resp.Peers = encodePeers(r.configurations.latest, r.trans)
	}

	// Check if we have an existing leader [who's not the candidate] and also
	// check the LeadershipTransfer flag is set. Usually votes are rejected if
	// there is a known leader. But if the leader initiated a leadership transfer,
	// vote!
	var candidate ServerAddress
	var candidateBytes []byte
	if len(req.Addr) > 0 {
		candidate = r.trans.DecodePeer(req.Addr)
		candidateBytes = req.Addr
	} else {
		candidate = r.trans.DecodePeer(req.Candidate)
		candidateBytes = req.Candidate
	}

	// For older raft version ID is not part of the packed message
	// We assume that the peer is part of the configuration and skip this check
	if len(req.ID) > 0 {
		candidateID := ServerID(req.ID)
		// if the Servers list is empty that mean the cluster is very likely trying to bootstrap,
		// Grant the vote
		if len(r.configurations.latest.Servers) > 0 && !inConfiguration(r.configurations.latest, candidateID) {
			r.logger.Warn("rejecting vote request since node is not in configuration",
				"from", candidate)
			return
		}
	}
	if leaderAddr, leaderID := r.LeaderWithID(); leaderAddr != "" && leaderAddr != candidate && !req.LeadershipTransfer {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from", candidate,
			"leader", leaderAddr,
			"leader-id", string(leaderID))
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		// Ensure transition to follower
		r.logger.Debug("lost leadership because received a requestVote with a newer term")
		r.setState(Follower)
		r.setCurrentTerm(req.Term)

		resp.Term = req.Term
	}

	// if we get a request for vote from a nonVoter  and the request term is higher,
	// step down and update term, but reject the vote request
	// This could happen when a node, previously voter, is converted to non-voter
	// The reason we need to step in is to permit to the cluster to make progress in such a scenario
	// More details about that in https://github.com/hashicorp/raft/pull/526
	if len(req.ID) > 0 {
		candidateID := ServerID(req.ID)
		if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) {
			r.logger.Warn("rejecting vote request since node is not a voter", "from", candidate)
			return
		}
	}
	// Check if we have voted yet
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm) // 注意: 下面用错误字符串判「未找到」, 是非结构化 error 接口留下的脆弱判据
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote term", "error", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote candidate", "error", err)
		return
	}

	// Check if we've voted in this election before
	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		r.logger.Info("duplicate requestVote for same term", "term", req.Term)
		if bytes.Equal(lastVoteCandBytes, candidateBytes) {
			r.logger.Warn("duplicate requestVote from", "candidate", candidate)
			resp.Granted = true
		}
		return
	}

	// Reject if their term is older
	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIdx,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	// Persist a vote for safety
	if err := r.persistVote(req.Term, candidateBytes); err != nil {
		r.logger.Error("failed to persist vote", "error", err)
		return
	}

	resp.Granted = true
	r.setLastContact()
}

// requestPreVote 处理 PreVote RPC: 不改变自身任期与状态, 只回答「若你发起竞选, 我是否愿投」, 用于抑制被隔离节点的任期膨胀。
// requestPreVote is invoked when we get a request Pre-Vote RPC call.
func (r *Raft) requestPreVote(rpc RPC, req *RequestPreVoteRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "requestPreVote"}, time.Now())
	r.observe(*req)

	// Setup a response
	resp := &RequestPreVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Granted:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Check if we have an existing leader [who's not the candidate] and also
	candidate := r.trans.DecodePeer(req.GetRPCHeader().Addr)
	candidateID := ServerID(req.ID)

	// if the Servers list is empty that mean the cluster is very likely trying to bootstrap,
	// Grant the vote
	if len(r.configurations.latest.Servers) > 0 && !inConfiguration(r.configurations.latest, candidateID) {
		r.logger.Warn("rejecting pre-vote request since node is not in configuration",
			"from", candidate)
		return
	}

	if leaderAddr, leaderID := r.LeaderWithID(); leaderAddr != "" && leaderAddr != candidate {
		r.logger.Warn("rejecting pre-vote request since we have a leader",
			"from", candidate,
			"leader", leaderAddr,
			"leader-id", string(leaderID))
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	if req.Term > r.getCurrentTerm() {
		// continue processing here to possibly grant the pre-vote as in a "real" vote this will transition us to follower
		r.logger.Debug("received a requestPreVote with a newer term, grant the pre-vote")
		resp.Term = req.Term
	}

	// if we get a request for a pre-vote from a nonVoter  and the request term is higher, do not grant the Pre-Vote
	// This could happen when a node, previously voter, is converted to non-voter
	if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) {
		r.logger.Warn("rejecting pre-vote request since node is not a voter", "from", candidate)
		return
	}

	// Reject if their term is older
	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting pre-vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Warn("rejecting pre-vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIdx,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	resp.Granted = true
}

// installSnapshot 处理 InstallSnapshot RPC: 把远端快照落盘、恢复进 FSM, 并重置 lastApplied / lastSnapshot 与配置; 只能在主线程调用。
// installSnapshot is invoked when we get a InstallSnapshot RPC call.
// We must be in the follower state for this, since it means we are
// too far behind a leader for log replay. This must only be called
// from the main thread.
func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "installSnapshot"}, time.Now())
	// Setup a response
	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		_, _ = io.Copy(io.Discard, rpc.Reader) // ensure we always consume all the snapshot data from the stream [see issue #212]
		rpc.Respond(resp, rpcErr)
	}()

	// Sanity check the version
	if req.SnapshotVersion < SnapshotVersionMin ||
		req.SnapshotVersion > SnapshotVersionMax {
		rpcErr = fmt.Errorf("unsupported snapshot version %d", req.SnapshotVersion)
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring installSnapshot request with older term than current term",
			"request-term", req.Term,
			"current-term", r.getCurrentTerm())
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		// Ensure transition to follower
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// Save the current leader
	if len(req.ID) > 0 {
		r.setLeader(r.trans.DecodePeer(req.Addr), ServerID(req.ID))
	} else {
		r.setLeader(r.trans.DecodePeer(req.Leader), ServerID(req.ID))
	}

	// Create a new snapshot
	var reqConfiguration Configuration
	var reqConfigurationIndex uint64
	if req.SnapshotVersion > 0 {
		reqConfiguration = DecodeConfiguration(req.Configuration)
		reqConfigurationIndex = req.ConfigurationIndex
	} else {
		reqConfiguration, rpcErr = decodePeers(req.Peers, r.trans)
		if rpcErr != nil {
			r.logger.Error("failed to install snapshot", "error", rpcErr)
			return
		}
		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, req.LastLogIndex, req.LastLogTerm,
		reqConfiguration, reqConfigurationIndex, r.trans)
	if err != nil {
		r.logger.Error("failed to create snapshot to install", "error", err)
		rpcErr = fmt.Errorf("failed to create snapshot: %v", err)
		return
	}

	// Separately track the progress of streaming a snapshot over the network
	// because this too can take a long time.
	countingRPCReader := newCountingReader(rpc.Reader)

	// Spill the remote snapshot to disk
	transferMonitor := startSnapshotRestoreMonitor(r.logger, countingRPCReader, req.Size, true)
	n, err := io.Copy(sink, countingRPCReader)
	transferMonitor.StopAndWait()
	if err != nil {
		_ = sink.Cancel()
		r.logger.Error("failed to copy snapshot", "error", err)
		rpcErr = err
		return
	}

	// Check that we received it all
	if n != req.Size {
		_ = sink.Cancel()
		r.logger.Error("failed to receive whole snapshot",
			"received", hclog.Fmt("%d / %d", n, req.Size))
		rpcErr = fmt.Errorf("short read")
		return
	}

	// Finalize the snapshot
	if err := sink.Close(); err != nil {
		r.logger.Error("failed to finalize snapshot", "error", err)
		rpcErr = err
		return
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// Restore snapshot
	future := &restoreFuture{ID: sink.ID()}
	future.ShutdownCh = r.shutdownCh
	future.init()
	select {
	case r.fsmMutateCh <- future:
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	// Wait for the restore to happen
	if err := future.Error(); err != nil {
		r.logger.Error("failed to restore snapshot", "error", err)
		rpcErr = err
		return
	}

	// Update the lastApplied so we don't replay old logs
	r.setLastApplied(req.LastLogIndex)

	// Update the last stable snapshot info
	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	// Restore the peer set
	r.setLatestConfiguration(reqConfiguration, reqConfigurationIndex)
	r.setCommittedConfiguration(reqConfiguration, reqConfigurationIndex)

	// Clear old logs if r.logs is a MonotonicLogStore. Otherwise compact the
	// logs. In both cases, log any errors and continue.
	if mlogs, ok := r.logs.(MonotonicLogStore); ok && mlogs.IsMonotonic() {
		if err := r.removeOldLogs(); err != nil {
			r.logger.Error("failed to reset logs", "error", err)
		}
	} else if err := r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Error("failed to compact logs", "error", err)
	}

	r.logger.Info("Installed remote snapshot")
	resp.Success = true
	r.setLastContact()
}

// setLastContact 把最近一次联系时间置为当前时刻。
// setLastContact is used to set the last contact time to now
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID
}

type preVoteResult struct {
	RequestPreVoteResponse
	voterID ServerID
}

// electSelf 发起正式选举: 递增当前任期、给自己投票并持久化, 再向所有 voter 并发发送 RequestVote; 只能在主线程调用。
// electSelf is used to send a RequestVote RPC to all peers, and vote for
// ourself. This has the side affecting of incrementing the current term. The
// response channel returned is used to wait for all the responses (including a
// vote for ourself). This must only be called from the main thread.
func (r *Raft) electSelf() <-chan *voteResult {
	// Create a response channel
	respCh := make(chan *voteResult, len(r.configurations.latest.Servers))

	// Increment the term
	newTerm := r.getCurrentTerm() + 1

	r.setCurrentTerm(newTerm)
	// Construct the request
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestVoteRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      newTerm,
		// this is needed for retro compatibility, before RPCHeader.Addr was added
		Candidate:          r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       lastIdx,
		LastLogTerm:        lastTerm,
		LeadershipTransfer: r.candidateFromLeadershipTransfer.Load(),
	}

	// Construct a function to ask for a vote
	askPeer := func(peer Server) {
		r.goFunc(func() {
			defer metrics.MeasureSince([]string{"raft", "candidate", "electSelf"}, time.Now())
			resp := &voteResult{voterID: peer.ID}
			err := r.trans.RequestVote(peer.ID, peer.Address, req, &resp.RequestVoteResponse)
			if err != nil {
				r.logger.Error("failed to make requestVote RPC",
					"target", peer,
					"error", err,
					"term", req.Term)
				resp.Term = req.Term
				resp.Granted = false
			}
			respCh <- resp
		})
	}

	// For each peer, request a vote
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				r.logger.Debug("voting for self", "term", req.Term, "id", r.localID)

				// Persist a vote for ourselves
				if err := r.persistVote(req.Term, req.Addr); err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return nil

				}
				// Include our own vote
				respCh <- &voteResult{
					RequestVoteResponse: RequestVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      req.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				r.logger.Debug("asking for vote", "term", req.Term, "from", server.ID, "address", server.Address)
				askPeer(server)
			}
		}
	}

	return respCh
}

// preElectSelf 发起预选举: 用 currentTerm+1 试探性征集 pre-vote, 但不改变自身任期与状态; 只能在主线程调用。
// preElectSelf is used to send a RequestPreVote RPC to all peers, and vote for
// ourself. This will not increment the current term. The
// response channel returned is used to wait for all the responses (including a
// vote for ourself).
// This must only be called from the main thread.
func (r *Raft) preElectSelf() <-chan *preVoteResult {

	// At this point transport should support pre-vote
	// but check just in case
	prevoteTrans, prevoteTransSupported := r.trans.(WithPreVote)
	if !prevoteTransSupported {
		panic("preElection is not possible if the transport don't support pre-vote")
	}

	// Create a response channel
	respCh := make(chan *preVoteResult, len(r.configurations.latest.Servers))

	// Propose the next term without actually changing our state
	newTerm := r.getCurrentTerm() + 1

	// Construct the request
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestPreVoteRequest{
		RPCHeader:    r.getRPCHeader(),
		Term:         newTerm,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Construct a function to ask for a vote
	askPeer := func(peer Server) {
		r.goFunc(func() {
			defer metrics.MeasureSince([]string{"raft", "candidate", "preElectSelf"}, time.Now())
			resp := &preVoteResult{voterID: peer.ID}

			err := prevoteTrans.RequestPreVote(peer.ID, peer.Address, req, &resp.RequestPreVoteResponse)

			// If the target server do not support Pre-vote RPC we count this as a granted vote to allow
			// the cluster to progress.
			if err != nil && strings.Contains(err.Error(), rpcUnexpectedCommandError) {
				r.logger.Error("target does not support pre-vote RPC, treating as granted",
					"target", peer,
					"error", err,
					"term", req.Term)
				resp.Term = req.Term
				resp.Granted = true
			} else if err != nil {
				r.logger.Error("failed to make requestVote RPC",
					"target", peer,
					"error", err,
					"term", req.Term)
				resp.Term = req.Term
				resp.Granted = false
			}
			respCh <- resp

		})
	}

	// For each peer, request a vote
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				r.logger.Debug("pre-voting for self", "term", req.Term, "id", r.localID)

				// cast a pre-vote for our self
				respCh <- &preVoteResult{
					RequestPreVoteResponse: RequestPreVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      req.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				r.logger.Debug("asking for pre-vote", "term", req.Term, "from", server.ID, "address", server.Address)
				askPeer(server)
			}
		}
	}

	return respCh
}

// persistVote 把本次投票(任期 + 候选人地址)持久化到 StableStore, 保证崩溃重启后不会重复投票。
// persistVote is used to persist our vote for safety.
func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

// setCurrentTerm 先持久化再更新内存中的当前任期; 持久化失败直接 panic(任期丢失会破坏选举安全)。
// setCurrentTerm is used to set the current term in a durable manner.
func (r *Raft) setCurrentTerm(t uint64) {
	// Persist to disk first
	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		panic(fmt.Errorf("failed to save current term: %v", err)) // 任期是选举安全的根: 存不下就必须立刻终止进程, 不能带着未知任期继续
	}
	r.raftState.setCurrentTerm(t)
}

// setState 更新当前状态; 任何状态切换都会先清空已知 leader, 因此 leader 必须在 setState 之后再设置。
// setState is used to update the current state. Any state
// transition causes the known leader to be cleared. This means
// that leader should be set only after updating the state.
func (r *Raft) setState(state RaftState) {
	r.setLeader("", "")
	oldState := r.getState()
	r.raftState.setState(state)
	if oldState != state {
		r.observe(state)
	}
}

// pickServer 在参与法定人数的 voter 中挑出复制进度(nextIndex)最靠前的那个作为领导权转移的默认目标; 只能在 leaderLoop 内调用。
// pickServer returns the follower that is most up to date and participating in quorum.
// Because it accesses leaderstate, it should only be called from the leaderloop.
func (r *Raft) pickServer() *Server {
	var pick *Server
	var current uint64
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID || server.Suffrage != Voter {
			continue
		}
		state, ok := r.leaderState.replState[server.ID]
		if !ok {
			continue
		}
		nextIdx := atomic.LoadUint64(&state.nextIndex)
		if nextIdx > current {
			current = nextIdx
			tmp := server
			pick = &tmp
		}
	}
	return pick
}

// initiateLeadershipTransfer 是领导权转移的入口: 把请求投给 leadershipTransferCh 交给主循环执行, 拒绝转移给自身。
// initiateLeadershipTransfer starts the leadership on the leader side, by
// sending a message to the leadershipTransferCh, to make sure it runs in the
// mainloop.
func (r *Raft) initiateLeadershipTransfer(id *ServerID, address *ServerAddress) LeadershipTransferFuture {
	future := &leadershipTransferFuture{ID: id, Address: address}
	future.init()

	if id != nil && *id == r.localID {
		err := fmt.Errorf("cannot transfer leadership to itself")
		r.logger.Info(err.Error())
		future.respond(err)
		return future
	}

	select {
	case r.leadershipTransferCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	default:
		return errorFuture{ErrEnqueueTimeout}
	}
}

// timeoutNow 处理 TimeoutNowRequest: 清空 leader、转为 candidate 并打上「由领导权转移触发」标志, 立即开始竞选。
// timeoutNow is what happens when a server receives a TimeoutNowRequest.
func (r *Raft) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	r.setLeader("", "")
	r.setState(Candidate)
	r.candidateFromLeadershipTransfer.Store(true) // 注意: 本函数不校验 req.Term, 它信任 leader 发来的 TimeoutNow
	rpc.Respond(&TimeoutNowResponse{}, nil)
}

// setLatestConfiguration 写入最新配置及其索引, 并刷新供主循环外读取的 latestConfiguration 副本。
// setLatestConfiguration stores the latest configuration and updates a copy of it.
func (r *Raft) setLatestConfiguration(c Configuration, i uint64) {
	r.configurations.latest = c
	r.configurations.latestIndex = i
	r.latestConfiguration.Store(c.Clone())
}

// setCommittedConfiguration 写入已提交配置及其索引。
// setCommittedConfiguration stores the committed configuration.
func (r *Raft) setCommittedConfiguration(c Configuration, i uint64) {
	r.configurations.committed = c
	r.configurations.committedIndex = i
}

// getLatestConfiguration 返回最新配置的副本(可从主循环外安全读取); 尚未设置过配置时返回空配置。
// getLatestConfiguration reads the configuration from a copy of the main
// configuration, which means it can be accessed independently from the main
// loop.
func (r *Raft) getLatestConfiguration() Configuration {
	// this switch catches the case where this is called without having set
	// a configuration previously.
	switch c := r.latestConfiguration.Load().(type) {
	case Configuration:
		return c
	default:
		return Configuration{}
	}
}
