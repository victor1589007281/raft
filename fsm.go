// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics/compat"
)

// FSM is implemented by clients to make use of the replicated log.
type FSM interface {
	// Apply is called once a log entry is committed by a majority of the cluster.
	//
	// Apply should apply the log to the FSM. Apply must be deterministic and
	// produce the same result on all peers in the cluster.
	//
	// The returned value is returned to the client as the ApplyFuture.Response.
	Apply(*Log) interface{}

	// Snapshot returns an FSMSnapshot used to: support log compaction, to
	// restore the FSM to a previous state, or to bring out-of-date followers up
	// to a recent log index.
	//
	// The Snapshot implementation should return quickly, because Apply can not
	// be called while Snapshot is running. Generally this means Snapshot should
	// only capture a pointer to the state, and any expensive IO should happen
	// as part of FSMSnapshot.Persist.
	//
	// Apply and Snapshot are always called from the same thread, but Apply will
	// be called concurrently with FSMSnapshot.Persist. This means the FSM should
	// be implemented to allow for concurrent updates while a snapshot is happening.
	//
	// Clients of this library should make no assumptions about whether a returned
	// Snapshot() will actually be stored by Raft. In fact it's quite possible that
	// any Snapshot returned by this call will be discarded, and that
	// FSMSnapshot.Persist will never be called. Raft will always call
	// FSMSnapshot.Release however.
	Snapshot() (FSMSnapshot, error)

	// Restore is used to restore an FSM from a snapshot. It is not called
	// concurrently with any other command. The FSM must discard all previous
	// state before restoring the snapshot.
	Restore(snapshot io.ReadCloser) error
}

// BatchingFSM extends the FSM interface to add an ApplyBatch function. This can
// optionally be implemented by clients to enable multiple logs to be applied to
// the FSM in batches. Up to MaxAppendEntries could be sent in a batch.
type BatchingFSM interface {
	// ApplyBatch is invoked once a batch of log entries has been committed and
	// are ready to be applied to the FSM. ApplyBatch will take in an array of
	// log entries. These log entries will be in the order they were committed,
	// will not have gaps, and could be of a few log types. Clients should check
	// the log type prior to attempting to decode the data attached. Presently
	// the LogCommand and LogConfiguration types will be sent.
	//
	// The returned slice must be the same length as the input and each response
	// should correlate to the log at the same index of the input. The returned
	// values will be made available in the ApplyFuture returned by Raft.Apply
	// method if that method was called on the same Raft node as the FSM.
	ApplyBatch([]*Log) []interface{}

	FSM
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot interface {
	// Persist should dump all necessary state to the WriteCloser 'sink',
	// and call sink.Close() when finished or call sink.Cancel() on error.
	Persist(sink SnapshotSink) error

	// Release is invoked when we are finished with the snapshot.
	Release()
}

// runFSM 是长期运行的 FSM 应用 goroutine: 用 select 串行消费 fsmMutateCh
// (批量提交 / 快照恢复)与 fsmSnapshotCh(快照请求), shutdownCh 关闭时退出。
//
// runFSM is a long running goroutine responsible for applying logs
// to the FSM. This is done async of other logs since we don't want
// the FSM to block our internal operations.
//
// 算法解读：
//
//	原理：单 goroutine + 三路 select 的事件循环。fsmMutateCh 上收 []*commitTuple
//	      走 applyBatch(内部再按 FSM 是否实现 BatchingFSM 分派到 ApplyBatch 或
//	      逐条 Apply), 收 *restoreFuture 走 restore; fsmSnapshotCh 上收快照请求走
//	      snapshot; shutdownCh 触发 return。saturation 计量本 goroutine 的忙闲。
//	关键不变量：**lastIndex/lastTerm 只在这个 goroutine 里推进**。快照点、恢复点
//	      与已应用点必须始终一致, 任何跨 goroutine 的更新都会让快照记下一个还没
//	      应用到的索引。接口注释里"Apply 与 Snapshot 永不并发"的承诺, 正是靠这个
//	      单线程循环兑现的(而 FSMSnapshot.Persist 允许与 Apply 并发)。
//	      另一条: LogBarrier 类型的条目永不进 FSM(被 shouldSend 过滤), 但**仍参与
//	      lastIndex 推进** —— lastBatchIndex 取自整批的最后一条, 而不是只取发送出去
//	      的那部分, 否则索引会停在一个已经处理过的位置之前。
//	代价与边界：批内一次 ApplyBatch, 开销线性于批大小; ApplyBatch 返回的响应条数
//	      与发送的日志条数不一致时直接 panic(视为 FSM 违约)。lastIndex==0 时快照
//	      请求返回 ErrNothingNewToSnapshot, 避免对空状态做快照。fsmMutateCh 上出现
//	      未知类型同样 panic —— 这是内部通道, 只可能来自库自身的 bug。
//	设计权衡：把 FSM 调用放在独立 goroutine 而不是 RPC 处理路径上, 是为了让慢 FSM
//	      不阻塞 raft 的内部操作; 代价是"提交完成"与"应用完成"解耦, 必须靠 future
//	      回填响应来把结果还给调用方。
func (r *Raft) runFSM() {
	// 12.7 FsmAsyncPersist tier (default off): when enabled, BatchingFSM.ApplyBatch
	// is expected to be pure bookkeeping (anchorBase / contiguous frontier) without
	// a file Sync on the critical path. The flag is validated in config.go and
	// plumbed via vraftd -fsm-async-persist; real FSM implementations opt in
	// explicitly. No behavior change when the flag is off.
	// 这里刻意只读一次、不做任何分支: 该开关的语义由 FSM 实现侧承担(纯记账、关键
	// 路径上不 Sync), raft 内核无需区分两种模式, 保留这一行只为让"已接线"可被检索。
	_ = r.config().FsmAsyncPersist
	var lastIndex, lastTerm uint64

	batchingFSM, batchingEnabled := r.fsm.(BatchingFSM)
	configStore, configStoreEnabled := r.fsm.(ConfigurationStore)

	// applySingle 逐条应用: 命令交给 FSM.Apply, 配置变更交给 ConfigurationStore,
	// 两条路径都会在末尾推进 lastIndex/lastTerm(除非配置存储缺失而提前 return)。
	applySingle := func(req *commitTuple) {
		// Apply the log if a command or config change
		var resp interface{}
		// Make sure we send a response
		defer func() {
			// Invoke the future if given
			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}()

		switch req.log.Type {
		case LogCommand:
			start := time.Now()
			resp = r.fsm.Apply(req.log)
			metrics.MeasureSince([]string{"raft", "fsm", "apply"}, start)

		case LogConfiguration:
			if !configStoreEnabled {
				// Return early to avoid incrementing the index and term for
				// an unimplemented operation.
				return
			}

			start := time.Now()
			configStore.StoreConfiguration(req.log.Index, DecodeConfiguration(req.log.Data))
			metrics.MeasureSince([]string{"raft", "fsm", "store_config"}, start)
		}

		// Update the indexes
		lastIndex = req.log.Index
		lastTerm = req.log.Term
	}

	// applyBatch 应用一批提交: FSM 实现了 BatchingFSM 就走一次 ApplyBatch, 否则
	// 退化成逐条 applySingle; 两条路径都要把响应按位置回填到各 req 的 future。
	applyBatch := func(reqs []*commitTuple) {
		// vraft: leader-side dispatch→apply latency. For batches the leader wrote
		// (futures non-nil), this approximates commit→apply end-to-end; follower
		// batches carry no dispatch timestamp and are skipped.
		if len(reqs) > 0 && reqs[0].future != nil && !reqs[0].future.dispatch.IsZero() {
			metrics.MeasureSince([]string{"raft", "leader", "dispatchToApply"}, reqs[0].future.dispatch)
		}
		if !batchingEnabled {
			for _, ct := range reqs {
				applySingle(ct)
			}
			return
		}

		// Only send LogCommand and LogConfiguration log types. LogBarrier types
		// will not be sent to the FSM.
		shouldSend := func(l *Log) bool {
			switch l.Type {
			case LogCommand, LogConfiguration:
				return true
			}
			return false
		}

		var lastBatchIndex, lastBatchTerm uint64
		sendLogs := make([]*Log, 0, len(reqs))
		for _, req := range reqs {
			if shouldSend(req.log) {
				sendLogs = append(sendLogs, req.log)
			}
			lastBatchIndex = req.log.Index
			lastBatchTerm = req.log.Term
		}

		var responses []interface{}
		if len(sendLogs) > 0 {
			start := time.Now()
			responses = batchingFSM.ApplyBatch(sendLogs)
			metrics.MeasureSince([]string{"raft", "fsm", "applyBatch"}, start)
			metrics.AddSample([]string{"raft", "fsm", "applyBatchNum"}, float32(len(reqs)))

			// 长度不符说明 FSM 违约(响应与日志没一一对应)。宁可 panic 也不能错位
			// 回填 —— 错位的响应会被当成另一条命令的结果返回给客户端。
			// Ensure we get the expected responses
			if len(sendLogs) != len(responses) {
				panic("invalid number of responses")
			}
		}

		// Update the indexes
		lastIndex = lastBatchIndex
		lastTerm = lastBatchTerm

		var i int
		for _, req := range reqs {
			var resp interface{}
			// If the log was sent to the FSM, retrieve the response.
			if shouldSend(req.log) {
				resp = responses[i]
				i++
			}

			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}
	}

	// restore 处理一次快照恢复: 打开快照 → 交给 fsmRestoreAndMeasure 恢复 → 用
	// 快照元数据里的 index/term 重置 lastIndex/lastTerm(即恢复点就是应用点)。
	restore := func(req *restoreFuture) {
		// Open the snapshot
		meta, source, err := r.snapshots.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("failed to open snapshot %v: %v", req.ID, err))
			return
		}
		defer func() { _ = source.Close() }()

		snapLogger := r.logger.With(
			"id", req.ID,
			"last-index", meta.Index,
			"last-term", meta.Term,
			"size-in-bytes", meta.Size,
		)

		// Attempt to restore
		if err := fsmRestoreAndMeasure(snapLogger, r.fsm, source, meta.Size); err != nil {
			req.respond(fmt.Errorf("failed to restore snapshot %v: %v", req.ID, err))
			return
		}

		// Update the last index and term
		lastIndex = meta.Index
		lastTerm = meta.Term
		req.respond(nil)
	}

	// snapshot 处理一次快照请求: 没有任何已应用内容时直接回 ErrNothingNewToSnapshot,
	// 否则调 FSM.Snapshot() 并把当前 (lastIndex, lastTerm) 一并交给调用方。
	snapshot := func(req *reqSnapshotFuture) {
		// Is there something to snapshot?
		if lastIndex == 0 {
			req.respond(ErrNothingNewToSnapshot)
			return
		}

		// Start a snapshot
		start := time.Now()
		snap, err := r.fsm.Snapshot()
		metrics.MeasureSince([]string{"raft", "fsm", "snapshot"}, start)

		// Respond to the request
		req.index = lastIndex
		req.term = lastTerm
		req.snapshot = snap
		req.respond(err)
	}

	saturation := newSaturationMetric([]string{"raft", "thread", "fsm", "saturation"}, 1*time.Second)

	for {
		saturation.sleeping()

		select {
		case ptr := <-r.fsmMutateCh:
			saturation.working()

			switch req := ptr.(type) {
			case []*commitTuple:
				applyBatch(req)

			case *restoreFuture:
				restore(req)

			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}

		case req := <-r.fsmSnapshotCh:
			saturation.working()

			snapshot(req)

		case <-r.shutdownCh:
			return
		}
	}
}

// fsmRestoreAndMeasure 包住一次 FSM.Restore 调用, 统一度量并上报恢复耗时。
// 无论成功还是失败, 关闭 source 都由调用方负责(本函数不 Close)。
//
// fsmRestoreAndMeasure wraps the Restore call on an FSM to consistently measure
// and report timing metrics. The caller is still responsible for calling Close
// on the source in all cases.
//
// 恢复度量: 用一个 countingReadCloser 包住 source, 让 startSnapshotRestoreMonitor
// 按已读字节数周期性地估算进度并打日志; defer 保证监控在返回前停止并等它收尾。
// 成功后记录 fsm.restore 的耗时与 lastRestoreDuration 仪表(单位毫秒)。注意度量只
// 在成功路径上记录 —— 失败时直接返回, 不会留下一次"半程"的耗时样本。
func fsmRestoreAndMeasure(logger hclog.Logger, fsm FSM, source io.ReadCloser, snapshotSize int64) error {
	start := time.Now()

	crc := newCountingReadCloser(source)

	// networkTransfer=false: 这里恢复的是本地快照文件, 不是从 leader 拉取
	monitor := startSnapshotRestoreMonitor(logger, crc, snapshotSize, false)
	defer monitor.StopAndWait()

	if err := fsm.Restore(crc); err != nil {
		return err
	}
	metrics.MeasureSince([]string{"raft", "fsm", "restore"}, start)
	metrics.SetGauge([]string{"raft", "fsm", "lastRestoreDuration"},
		float32(time.Since(start).Milliseconds()))

	return nil
}
