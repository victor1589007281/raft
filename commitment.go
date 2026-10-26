// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sort"
	"sync"
)

// Commitment is used to advance the leader's commit index. The leader and
// replication goroutines report in newly written entries with match(), and
// this notifies on commitCh when the commit index has advanced.
type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	matchIndexes map[ServerID]uint64
	// a quorum stores up through this log entry. monotonically increases.
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startIndex uint64
	// self is the local server ID. With async leader persist, the leader's own
	// match is the ceiling for the commit index: an entry may only be
	// committed once it is durable on the leader (see recalculate).
	self ServerID
}

// newCommitment 为新任期构造 commitment: 把配置中所有 voter 的 matchIndex
// 初始化为 0, 并记下本任期首条索引 startIndex 与本地服务器 ID self。
// 每次本节点当选 leader 都新建一个 —— 提交状态不跨任期继承。
//
// newCommitment returns a commitment struct that notifies the provided
// channel when log entries have been committed. A new commitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startIndex' is the first index created in this term (see
// its description above).
// 'self' is the local server ID, used to cap the commit index at the leader's
// own durable log.
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64, self ServerID) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
		self:         self,
	}
}

// setConfiguration 在成员变更后重建 matchIndexes: 仍在配置中的 voter 保留既有
// 匹配进度, 新 voter 从 0 起算(缺省值), 建完立即重算提交索引。
//
// Called when a new cluster membership configuration is created: it will be
// used to determine commitment from now on. 'configuration' is the servers in
// the cluster.
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// getCommitIndex 返回当前提交索引(加锁读, 由 leaderLoop 在 commitCh 通知后调用)。
//
// Called by leader after commitCh is notified
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// match 记录某服务器已持久化到 matchIndex: 只接受严格变大的更新, 且只有变大时
// 才触发 recalculate —— 匹配进度因此只前进不后退。
//
// 算法解读：
//
//	原理：matchIndexes 是「已确认存到」的下界。乱序到达的旧应答(更小的 index)
//	  被静默丢弃且不重算; 不在 map 里的服务器(learner、已移除者)整条忽略。
//	关键不变量：单调性必须在**入口**守住 —— 若允许回退, 依据旧值提交过的条目
//	  会被重新判定为未提交, 直接违背 Raft 的提交安全性。
//	代价与边界：本地节点 self 走同一入口(leader 自己写完日志后调用
//	  match(self, idx)), 于是 quorum 计算天然把 leader 自己算进去,
//	  不必为「本地匹配」另开一条不走网络的路径。
//	设计权衡：单调性检查放在这里而不是 recalculate, 是为了让重复/陈旧应答连锁内
//	  的排序都不做 —— 流水线复制下乱序应答是常态。
//
// Match is called once a server completes writing entries to disk: either the
// leader has written the new entry or a follower has replied to an
// AppendEntries RPC. The given server's disk agrees with this server's log up
// through the given index.
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

// recalculate 由 matchIndexes 重算提交索引, 推进时通知 commitCh(必须在持锁下调用)。
//
// 算法解读：
//
//	原理：把每个 voter 的 matchIndex 升序排序后取下标 (n-1)/2 的元素 —— 它表示
//	  「至少有 ⌈(n+1)/2⌉ 个节点已存到该索引」, 这正是 quorum 的定义, 故它就是
//	  候选提交点; 再要求 >= startIndex(本任期首条)才允许提交, 即 Raft
//	  「不能靠多数派提交往届条目」的规则。
//	关键不变量：① matchIndex 只增 ⇒ 提交索引单调; ② 参与排序的集合必须恰好是当前
//	  配置的 voter(learner/已移除者不进 map; 混入非投票者会稀释多数派判据, 把提交
//	  点算低); ③ self 封顶不可省 —— leader 异步持久化时自身 match 落后于 follower,
//	  不加封顶就会提交尚未落盘的条目(见下面 vraft 分支)。
//	代价与边界：每次有效 match 都重排一次 O(n log n) 并分配一个切片; 集群规模很小
//	  可忽略, 但它跑在锁内, 高吞吐下是热点。
//	设计权衡：排序取中位数而非维护「第 k 大」的堆, 因为 n 小、实现简单更值钱;
//	  也不做增量更新 —— 一次 match 只改一个 key, 增量的复杂度换不来收益。
//
// Internal helper to calculate new commitIndex from matchIndexes.
// Must be called with lock held.
func (c *commitment) recalculate() {
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, idx := range c.matchIndexes {
		matched = append(matched, idx)
	}
	sort.Sort(uint64Slice(matched))
	quorumMatchIndex := matched[(len(matched)-1)/2]

	// vraft (async leader persist): the commit index must never advance past
	// the leader's own durable log. When the leader persists asynchronously its
	// own match lags behind followers' matches, so followers alone could
	// otherwise push the quorum index past what the leader has fsynced — an
	// entry committed but not durable on the leader would be lost on leader
	// crash (etcd issue #18). Capping at the leader's match closes this; when
	// the leader's async fsync completes, match() advances it and re-runs this
	// recalculation, unblocking the commit.
	if selfIdx, hasVote := c.matchIndexes[c.self]; hasVote && selfIdx < quorumMatchIndex {
		quorumMatchIndex = selfIdx
	}

	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		// 非阻塞通知: commitCh 容量 1 且用 asyncNotifyCh, 消费慢时丢弃通知而不是
		// 阻塞复制 goroutine —— 丢掉的只是「提交索引变了」这个提示, leaderLoop
		// 醒来后读到的仍是当前最新值, 不会漏掉任何已提交条目。
		asyncNotifyCh(c.commitCh)
	}
}
