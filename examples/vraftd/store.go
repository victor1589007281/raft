// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

// Package vraftd is the vRaft demo application: a small replicated KV store on
// top of the vraft fork of hashicorp/raft. It exists to (a) exercise the vraft
// write-path features (BatchWindow, async leader persist, single WAL) in a real
// file-backed process and (b) serve as the container payload for the kind k8s
// deployment.
//
// The durable stores live in the library-level package
// github.com/hashicorp/raft/filestore (12.7/12.9/12.14 capabilities: single
// WAL v2 format, CRC torn-tail detection, sparse LSN index, io_uring chain).
// This file keeps only the demo FSM.
package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// ---------------------------------------------------------------------------
// KVFSM: the replicated state machine.
//
// Commands are JSON: {"op":"set","k":...,"v":...} and {"op":"del","k":...}.
// The applied counter lets a benchmark verify that every issued command was
// actually applied by a quorum.
type KVFSM struct {
	mu      sync.RWMutex
	kv      map[string]string
	applied uint64
}

// newKVFSM 构造一个空的 KVFSM, kv 映射已初始化, 可以直接接受 Apply。
func newKVFSM() *KVFSM {
	return &KVFSM{kv: make(map[string]string)}
}

type kvCmd struct {
	Op string `json:"op"`
	K  string `json:"k"`
	V  string `json:"v"`
}

// Apply 在 FSM 上执行一条已提交的日志条目: 解析 JSON 命令, 更新 kv 映射并返回回执。
//
// 算法解读：
//
//	原理：raft 保证同一节点的 Apply 按日志顺序串行调用(runFSM 单 goroutine), 这里
//	      仍然加写锁, 是因为 Get/Dump/Snapshot 会被 HTTP handler 并发读取; applied
//	      计数与 kv 更新在同一把锁里完成, 因此"计数已含本条"与"kv 已可见"对外是
//	      同一个原子瞬间。
//	关键不变量：解码失败发生在加锁之前, 那条路径直接返回 error 且**不**递增 applied,
//	      所以 applied 恒等于"成功解码过的条目数", 而不是"真正改动过状态的条目数"——
//	      未知 op 会递增 applied 却不碰 kv。这个偏差在 /readall 里能看出来。
//	代价与边界：返回的 error 只是 FSM 层的应答, 不会让 ApplyFuture.Error() 非 nil
//	      (fsm.go 里 future.respond(nil) 是写死的), 所以调用方只能从 Response() 里
//	      看到它; 走 JSON 编码后 error 会被序列化成 {}。
//	设计权衡：整条命令按 JSON 解析而不是定长二进制, 是为了让 /apply 的入参和
//	      bench 生成的命令共用一种格式; 代价是每条命令多一次反射式解码。
//
// 注意：-fsm-async-persist 所描述的"ApplyBatch 纯记账 + 内存水位推进"在这个 FSM 上
// **没有体现**——KVFSM 只实现了 FSM 接口, 不是 BatchingFSM, 而 fsm.go 里对
// FsmAsyncPersist 只有一句 `_ = r.config().FsmAsyncPersist` 就直接丢弃了。也就是说
// vraftd 打开该开关不会改变任何行为 (见汇报)。
func (f *KVFSM) Apply(l *raft.Log) interface{} {
	var cmd kvCmd
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("bad command: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied++
	switch cmd.Op {
	case "set":
		f.kv[cmd.K] = cmd.V
		return cmd.V
	case "del":
		delete(f.kv, cmd.K)
		return cmd.K
	}
	return fmt.Errorf("unknown op %q", cmd.Op)
}

// Applied 返回 FSM 累计应用过的命令条数, 读锁保护, 供基准与 /readall 交叉核对。
//
// 注意：这是 FSM 自己的命令计数, 不是 raft 的 applied index——noop/config 条目
// 会推进 raft 的 applied index 却永远到不了 FSM, 两者必然分叉 (见 handleRead)。
func (f *KVFSM) Applied() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// Get 读取单个键, 第二个返回值为是否命中; 读锁保护, 供 /read 直接调用。
func (f *KVFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.kv[key]
	return v, ok
}

// Dump 返回 kv 的一份深拷贝快照(map 本身是新分配的, 值是不可变 string),
// 这样调用方序列化时不必再持有读锁。
func (f *KVFSM) Dump() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.kv))
	for k, v := range f.kv {
		out[k] = v
	}
	return out
}

// kvSnapshot is a point-in-time copy of the FSM for raft.FileSnapshotStore.
type kvSnapshot struct {
	kv map[string]string
}

// Snapshot 在写锁之外做一次全量拷贝, 返回的 kvSnapshot 可以在 Apply 继续推进的
// 同时被慢慢写盘。
//
// 算法解读：
//
//	原理：raft 的 FSM 快照接口刻意拆成两步——Snapshot() 必须在**极短时间**内返回
//	      一个不可变视图, 真正的慢操作(序列化+落盘)留给之后可能被并发调用的
//	      Persist()。这里用 RLock 下的一次 map 拷贝满足这个约束: 拷贝期间 Apply
//	      被阻塞(写锁互斥), 拷贝一结束锁就释放。
//	关键不变量：返回的 kvSnapshot 必须与后续的 Apply **完全解耦**——所以这里拷的是
//	      map 而不是保留引用。若直接返回 f.kv 的引用, Persist 序列化时就会读到
//	      中途被改的 map, 快照就不是某个时刻的一致切面, 且 map 并发读写会直接崩。
//	代价与边界：每次快照 O(len(kv)) 内存与时间, 与状态量成正比; 大状态下这一步
//	      本身会成为一次可观测的停顿, 这是"全量内存拷贝"方案的固有成本。
//	设计权衡：也可以只记录 raft index 再在 Persist 里回放, 但那样需要保留历史
//	      版本; 演示程序选最直白的"拷贝即快照"。
func (f *KVFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	snap := make(map[string]string, len(f.kv))
	for k, v := range f.kv {
		snap[k] = v
	}
	return &kvSnapshot{kv: snap}, nil
}

// Persist 把快照内容 gob 编码写入 sink, 失败时取消 sink, 成功时关闭它。
//
// 算法解读：
//
//	原理：raft 的契约是"要么 Close 要么 Cancel"——Close 才会让 SnapshotStore
//	      把临时文件改名成正式快照并让 raft 推进它的快照索引; Cancel 则丢弃。
//	      这里用一个立即执行的闭包包住编码, 好让"编码失败"与"Close 失败"两条
//	      出口分开: 编码失败必须先 Cancel 再返回, 绝不能 Close 一个半截文件。
//	关键不变量：sink.Close() 的返回值是快照是否真正持久化的**唯一**判据, 不能丢;
//	      编码过程中没有任何锁被持有, 因为 s.kv 是 Snapshot() 时拷出来的私有副本。
//	代价与边界：gob 编码走的是反射, 比手写二进制慢, 且 gob 的格式与结构体字段名
//	      绑定; 换个语言/换个字段名读老快照会失败——演示程序接受这个代价。
//	设计权衡：gob 而不是 JSON, 是因为 map[string]string 用 gob 可以省掉键的引号
//	      与转义, 也天然支持未来往快照里加结构体字段。
func (s *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := func() error {
		enc := gob.NewEncoder(sink)
		return enc.Encode(s.kv)
	}(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release 是 FSMSnapshot 的空实现: 快照只持有一份内存 map, 没有 fd 或临时文件
// 需要释放, 交给 GC 即可。
func (s *kvSnapshot) Release() {}

// Restore 从快照流里整体重建 kv 映射, 并让 applied 计数跟上快照里的键数。
//
// 算法解读：
//
//	原理：Restore 只在两种时刻被调用——启动时从最新快照装载, 或 follower 落后
//	      太多而收到 leader 发来的 InstallSnapshot。两种情况都要求"快照内容
//	      整体替换当前状态", 因此这里直接赋值 f.kv 而不是合并。
//	关键不变量：解码失败时**不能**部分写入 f.kv——gob 解码到局部变量 kv, 只有
//	      整个解码成功才赋值, 所以失败时旧状态原封不动, raft 会让节点重试装载。
//	      另一个不变量是 Restore 与 Apply 互斥: 全程持写锁, 否则恢复中途到达的
//	      命令会丢。
//	代价与边界：applied 被重置为 len(kv) 是**近似**的——快照里没有命令条数信息,
//	      键数只是下界(重复 set 同一个键会让真实条数大于键数)。它只用于 /readall
//	      和基准的交叉核对, 不参与任何正确性判据。
//	设计权衡：Restore 不返回 error 以外的信息, 也不做校验和比对; 完整性由快照
//	      存储层(FileSnapshotStore 的 meta + CRC)负责, FSM 层只信它给的字节。
func (f *KVFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	var kv map[string]string
	if err := gob.NewDecoder(rc).Decode(&kv); err != nil {
		return err
	}
	f.kv = kv
	f.applied = uint64(len(kv))
	return nil
}
