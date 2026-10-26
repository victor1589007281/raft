// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Future is used to represent an action that may occur in the future.
type Future interface {
	// Error blocks until the future arrives and then returns the error status
	// of the future. This may be called any number of times - all calls will
	// return the same value, however is not OK to call this method twice
	// concurrently on the same Future instance.
	// Error will only return generic errors related to raft, such
	// as ErrLeadershipLost, or ErrRaftShutdown. Some operations, such as
	// ApplyLog, may also return errors from other methods.
	Error() error
}

// IndexFuture is used for future actions that can result in a raft log entry
// being created.
type IndexFuture interface {
	Future

	// Index holds the index of the newly applied log entry.
	// This must not be called until after the Error method has returned.
	Index() uint64
}

// ApplyFuture is used for Apply and can return the FSM response.
type ApplyFuture interface {
	IndexFuture

	// Response returns the FSM response as returned by the FSM.Apply method. This
	// must not be called until after the Error method has returned.
	// Note that if FSM.Apply returns an error, it will be returned by Response,
	// and not by the Error method, so it is always important to check Response
	// for errors from the FSM.
	Response() interface{}
}

// ConfigurationFuture is used for GetConfiguration and can return the
// latest configuration in use by Raft.
type ConfigurationFuture interface {
	IndexFuture

	// Configuration contains the latest configuration. This must
	// not be called until after the Error method has returned.
	Configuration() Configuration
}

// SnapshotFuture is used for waiting on a user-triggered snapshot to complete.
type SnapshotFuture interface {
	Future

	// Open is a function you can call to access the underlying snapshot and
	// its metadata. This must not be called until after the Error method
	// has returned.
	Open() (*SnapshotMeta, io.ReadCloser, error)
}

// LeadershipTransferFuture is used for waiting on a user-triggered leadership
// transfer to complete.
type LeadershipTransferFuture interface {
	Future
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

// Error 返回该 future 预置的静态错误, 立即返回、永不阻塞。
func (e errorFuture) Error() error {
	return e.err
}

// Response 恒返回 nil —— errorFuture 只承载错误, 没有 FSM 响应体。
func (e errorFuture) Response() interface{} {
	return nil
}

// Index 恒返回 0 —— 静态错误 future 不对应任何日志条目。
func (e errorFuture) Index() uint64 {
	return 0
}

// deferError 可嵌入到各类 future 中, 提供一个「稍后才有」的错误结果:
// 结果由 respond 投递、由 Error 阻塞取回, Raft 关闭时以 ErrRaftShutdown 兜底。
//
// deferError can be embedded to allow a future
// to provide an error in the future.
type deferError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

// init 惰性创建 errCh, 必须在 Error / respond 之前调用。
// 容量 1 是刻意的: 结果常常在等待方到来之前就已 respond(leaderLoop 先解 future
// 再交接), 有缓冲才能让投递方不被尚未出现的等待方阻塞。
func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

// Error 阻塞直到 future 被 respond 或 Raft 关闭, 并把结果缓存进 d.err。
// 结果一经取回即短路返回(重复调用返回同一个值); 接口约定同一实例不可并发调用。
//
// 算法解读：
//
//	原理：init 建一条容量 1 的 errCh, 主线程或复制 goroutine 通过 respond 投递
//	  结果, 调用方在这里 select 等待「结果到达」与「进程关闭」两个事件。
//	关键不变量：respond 必须幂等(responded 标志), 否则二次投递会 panic 在已关闭
//	  的 channel 上; errCh 容量必须 >= 1, 否则「先 respond 后等待」的路径会把
//	  投递方一起阻塞住。
//	代价与边界：respond(nil) 不会被 `d.err != nil` 短路命中, 而是落到 select 上读
//	  一条已关闭的 channel —— 依然立即返回 nil, 与缓存路径行为一致。
//	设计权衡：用 channel 而不是 sync.Cond/WaitGroup, 因为调用方要同时等「结果」
//	  与「关闭」两件事, select 天然表达这种二选一。
func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = ErrRaftShutdown
	}
	return d.err
}

// respond 投递 future 的结果并关闭 errCh; 保证只生效一次(responded 幂等护栏)。
// 在 init 之前调用或重复调用都是安全空操作 —— 这让「先 respond 再交接 future」
// 的路径不必关心接收方是否已就绪。
func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// There are several types of requests that cause a configuration entry to
// be appended to the log. These are encoded here for leaderLoop() to process.
// This is internal to a single server.
type configurationChangeFuture struct {
	logFuture
	req configurationChangeRequest
}

// catchupCheckFuture is a single poll of a learner's catchup state, resolved by
// the main thread in leaderLoop. It reports whether the server's confirmed
// matched index has reached the leader's last log index.
type catchupCheckFuture struct {
	deferError
	serverID ServerID
	caughtUp bool
}

// promotionFuture is the result of PromoteToVoter: it resolves once the learner
// has caught up and the AddVoter configuration entry is committed.
type promotionFuture struct {
	deferError
	index uint64
}

// Index 返回 AddVoter 配置条目提交后的日志索引(提交前为 0)。
func (p *promotionFuture) Index() uint64 {
	return p.index
}

// Response 恒返回 nil —— 晋升不产生 FSM 响应。
func (p *promotionFuture) Response() interface{} {
	return nil
}

// resolvedIndexFuture is a pre-resolved IndexFuture, used by PromoteToVoter to
// return the current commit index when the server is already a voter.
type resolvedIndexFuture struct {
	index uint64
}

// Error 恒返回 nil —— 该 future 在构造时结果即已确定。
func (r *resolvedIndexFuture) Error() error {
	return nil
}

// Response 恒返回 nil。
func (r *resolvedIndexFuture) Response() interface{} {
	return nil
}

// Index 返回构造时冻结的索引(服务器已是 voter 时, 即当时的 commit index)。
func (r *resolvedIndexFuture) Index() uint64 {
	return r.index
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	deferError

	// configuration is the proposed bootstrap configuration to apply.
	configuration Configuration
}

// logFuture 表示一次 Apply 或配置变更: 条目在日志中就位后由 leaderLoop 解出结果,
// 提交之前 Error 一直阻塞; 领导权丢失或 Raft 关闭时分别以 ErrLeadershipLost /
// ErrRaftShutdown 解出。
//
// logFuture is used to apply a log entry and waits until
// the log is considered committed.
type logFuture struct {
	deferError
	log      Log
	response interface{}
	dispatch time.Time
}

// Response 返回 FSM.Apply 的返回值; 必须在 Error 返回之后调用。
func (l *logFuture) Response() interface{} {
	return l.response
}

// Index 返回该条目在日志中的索引。
func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

// Error 阻塞至 Raft 完全停机, 并在 transport 实现 WithClose 时顺带关闭它。
func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.waitShutdown()
	if closeable, ok := s.raft.trans.(WithClose); ok {
		_ = closeable.Close()
	}
	return nil
}

// userSnapshotFuture is used for waiting on a user-triggered snapshot to
// complete.
type userSnapshotFuture struct {
	deferError

	// opener is a function used to open the snapshot. This is filled in
	// once the future returns with no error.
	opener func() (*SnapshotMeta, io.ReadCloser, error)
}

// Open 打开用户快照, 返回其元数据与内容读取器; 只能成功取一次 ——
// 取回后立即把 opener 置空, 防止重复打开(重复打开并不安全)。
//
// Open is a function you can call to access the underlying snapshot and its
// metadata.
func (u *userSnapshotFuture) Open() (*SnapshotMeta, io.ReadCloser, error) {
	if u.opener == nil {
		return nil, nil, fmt.Errorf("no snapshot available")
	}
	// Invalidate the opener so it can't get called multiple times,
	// which isn't generally safe.
	defer func() {
		u.opener = nil
	}()
	return u.opener()
}

// userRestoreFuture is used for waiting on a user-triggered restore of an
// external snapshot to complete.
type userRestoreFuture struct {
	deferError

	// meta is the metadata that belongs with the snapshot.
	meta *SnapshotMeta

	// reader is the interface to read the snapshot contents from.
	reader io.Reader
}

// reqSnapshotFuture is used for requesting a snapshot start.
// It is only used internally.
type reqSnapshotFuture struct {
	deferError

	// snapshot details provided by the FSM runner before responding
	index    uint64
	term     uint64
	snapshot FSMSnapshot
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	deferError
	ID string
}

// verifyFuture is used to verify the current node is still
// the leader. This is to prevent a stale read.
type verifyFuture struct {
	deferError
	notifyCh   chan *verifyFuture
	quorumSize int
	votes      int
	voteLock   sync.Mutex
}

// leadershipTransferFuture is used to track the progress of a leadership
// transfer internally.
type leadershipTransferFuture struct {
	deferError

	ID      *ServerID
	Address *ServerAddress
}

// configurationsFuture is used to retrieve the current configurations. This is
// used to allow safe access to this information outside of the main thread.
type configurationsFuture struct {
	deferError
	configurations configurations
}

// Configuration 返回 Raft 当前在用的最新配置(由 leaderLoop 主线程填好, 外部可安全读)。
//
// Configuration returns the latest configuration in use by Raft.
func (c *configurationsFuture) Configuration() Configuration {
	return c.configurations.latest
}

// Index 返回该最新配置对应的日志索引。
//
// Index returns the index of the latest configuration in use by Raft.
func (c *configurationsFuture) Index() uint64 {
	return c.configurations.latestIndex
}

// vote 为一次领导权校验投一票; 票数达到 quorum(或收到反对票)时唤醒等待方。
// 每个 future 只唤醒一次: 投出后把 notifyCh 置 nil, 之后的票被直接丢弃。
//
// vote is used to respond to a verifyFuture.
// This may block when responding on the notifyCh.
func (v *verifyFuture) vote(leader bool) {
	v.voteLock.Lock()
	defer v.voteLock.Unlock()

	// Guard against having notified already
	if v.notifyCh == nil {
		return
	}

	if leader {
		v.votes++
		if v.votes >= v.quorumSize {
			v.notifyCh <- v
			v.notifyCh = nil
		}
	} else {
		v.notifyCh <- v
		v.notifyCh = nil
	}
}

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse
}

// Start 返回该流水线请求发出的时刻, 用于统计 RPC 往返耗时。
func (a *appendFuture) Start() time.Time {
	return a.start
}

// Request 返回该流水线请求的 AppendEntries 参数。
func (a *appendFuture) Request() *AppendEntriesRequest {
	return a.args
}

// Response 返回该流水线请求收到的应答。
func (a *appendFuture) Response() *AppendEntriesResponse {
	return a.resp
}
