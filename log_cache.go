// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"sync"
)

// LogCache 是包在任意 LogStore 外面的内存环形缓冲, 用来缓存最近写入的日志条目;
// 底层存储自己不缓存时, 它能靠免掉近期条目的磁盘 I/O 换来可观加速。
//
// LogCache wraps any LogStore implementation to provide an
// in-memory ring buffer. This is used to cache access to
// the recently written entries. For implementations that do not
// cache themselves, this can provide a substantial boost by
// avoiding disk I/O on recent entries.
type LogCache struct {
	store LogStore

	cache []*Log
	l     sync.RWMutex
}

// NewLogCache 按给定容量与后端 store 建一个 LogCache; capacity <= 0 直接报错
// —— 容量为 0 会让 GetLog 里的 `idx % len(cache)` 除零 panic, 所以必须挡在这里。
//
// NewLogCache is used to create a new LogCache with the
// given capacity and backend store.
func NewLogCache(capacity int, store LogStore) (*LogCache, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}
	c := &LogCache{
		store: store,
		cache: make([]*Log, capacity),
	}
	return c, nil
}

// IsMonotonic 转发底层 store 的单调性声明: 底层实现了 MonotonicLogStore 就返回
// 它的答案, 否则返回 false(即"不保证单调", 让 raft 走保守路径)。
//
// IsMonotonic implements the MonotonicLogStore interface. This is a shim to
// expose the underlying store as monotonically indexed or not.
//
// 算法解读：
//
//	原理：LogCache 是纯缓存壳, 不改变索引语义, 所以单调性必须由被包裹的 store
//	      决定; 这里只做一次类型断言转发, 底层没实现就返回 false。
//	关键不变量：false 的含义是"不知道/不保证", 不是"不单调"。raft 只在返回 true
//	      时才走优化路径(见 raft.go: 快照恢复后直接 removeOldLogs, 而不是按空洞
//	      触发 follower 重新同步, 并依赖存储对 DeleteLogs 全删有高效实现)。
//	      因此误报 true 会跳过必要的检查, 误报 false 只是少一条优化 —— 缺省取
//	      false 是安全方向。
//	代价与边界：O(1), 一次接口断言; 底层未实现该接口时无法探测, 一律 false。
//	设计权衡：宁可少优化也不冒错判风险 —— 单调性是存储的内部实现细节, 缓存层
//	      无权替它承诺; 而把判断下沉到 store 又能让 filestore 这类位置式追加存储
//	      主动声明单调、从而吃到优化。
func (c *LogCache) IsMonotonic() bool {
	if store, ok := c.store.(MonotonicLogStore); ok {
		return store.IsMonotonic()
	}

	return false
}

// GetLog 先查内存环形缓冲(按 idx % 容量直接映射), 命中且索引相符就整条拷出,
// 未命中则转发给底层 store。
//
// 算法解读：
//
//	原理：cache[idx % len(cache)] 是直接映射(不是 LRU): 同一槽位会被后写入的索引
//	      覆盖, 所以命中判定必须同时满足"槽位非空"且"槽内条目的 Index == idx";
//	      只看非空会把另一个索引的条目当成命中, 读出错日志。
//	关键不变量：*log = *cached 是浅拷贝 —— Data/Extensions 切片仍与缓存共享底层
//	      数组, 调用方不应就地改写; 缓存只在 StoreLogs 成功后写入, 所以这里绝不会
//	      命中一条从未落盘的条目。读锁只护住"取槽位"这一步, 取到指针即释放:
//	      条目一旦入缓存其字段就不再被缓存层改动, 故后续解引用无数据竞争。
//	代价与边界：命中 O(1) 且几乎无锁竞争; 未命中退化为底层一次读, 与不用缓存等价。
//	设计权衡：直接映射而非真 LRU —— 容量固定、无链表无额外分配, 而 raft 的访问
//	      模式(近期连续索引)对直接映射已经足够友好。
func (c *LogCache) GetLog(idx uint64, log *Log) error {
	// Check the buffer for an entry
	c.l.RLock()
	cached := c.cache[idx%uint64(len(c.cache))]
	c.l.RUnlock()

	// Check if entry is valid
	if cached != nil && cached.Index == idx {
		*log = *cached
		return nil
	}

	// Forward request on cache miss
	return c.store.GetLog(idx, log)
}

// GetLogRaw 是 12.8.2 的裸字节快路径: 底层存储若暴露 GetLogRaw(如 InmemStore /
// 未来的统一 WAL), 就直接返回线上字节, 省掉一次解码/组装; 否则回退到 GetLog。
//
// GetLogRaw is the 12.8.2 raw-bytes fast path: if the backing store exposes
// GetLogRaw (e.g. InmemStore / future unified WAL), return the wire bytes
// without an extra decode/copy. Falls back to GetLog when unavailable.
//
// 算法解读：
//
//	原理：先做一次匿名接口断言 store.(interface{ GetLogRaw(uint64) ([]byte, error) }),
//	      命中就把整个调用委托下去; 未命中则 GetLog 拿到 *Log 再返回它的 Data。
//	关键不变量：两条路径返回的都是同一条日志的 Data 字节, 但**所有权不同** ——
//	      快路径的返回值由存储决定是否与内部缓冲共享, 调用方一律不得就地修改;
//	      回退路径返回的是栈上临时 Log 的 Data 字段。两条路径都不含索引/任期等
//	      元数据, 需要完整条目时不能用它。
//	代价与边界：快路径省掉的是"把整条 Log 解码/组装"的开销(本仓 filestore 的实现
//	      仍会复制一份字节, 所以省的不是这次 memcpy); 回退路径多一次 GetLog, 可能
//	      触发一次磁盘 I/O。
//	设计权衡：用匿名接口做能力探测, 而不是把 GetLogRaw 加进 LogStore 接口 ——
//	      后者会强迫所有既有实现(包括第三方)都补一个方法, 破坏兼容。
func (c *LogCache) GetLogRaw(idx uint64) ([]byte, error) {
	if s, ok := c.store.(interface{ GetLogRaw(uint64) ([]byte, error) }); ok {
		return s.GetLogRaw(idx)
	}
	var l Log
	if err := c.GetLog(idx, &l); err != nil {
		return nil, err
	}
	return l.Data, nil
}

// StoreLog 是 StoreLogs 的单条包装, 复用同一条"先落盘、成功后才入缓存"的路径。
func (c *LogCache) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

// StoreLogs 先写底层 store, 成功之后才把这批条目放进环形缓冲。
//
// 关键约束：顺序不能反 —— 先落盘再缓存, 保证缓存里不会出现底层其实没有的条目
// (否则 GetLog 命中就会读出一条凭空存在的日志); 失败时原样包一层错误返回,
// 并且一条都不入缓存。
func (c *LogCache) StoreLogs(logs []*Log) error {
	err := c.store.StoreLogs(logs)
	// Insert the logs into the ring buffer, but only on success
	if err != nil {
		return fmt.Errorf("unable to store logs within log store, err: %q", err)
	}
	c.l.Lock()
	for _, l := range logs {
		c.cache[l.Index%uint64(len(c.cache))] = l
	}
	c.l.Unlock()
	return nil
}

// FirstIndex 直接转发底层 store 的 FirstIndex —— 缓存是纯加速层, 不参与首索引判定。
func (c *LogCache) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

// LastIndex 直接转发底层 store 的 LastIndex —— 与 FirstIndex 同理, 缓存不改变索引语义。
func (c *LogCache) LastIndex() (uint64, error) {
	return c.store.LastIndex()
}

// DeleteRange 先整体作废缓存(重新分配一个等长切片), 再转发底层删除。
//
// 为什么整体清空而不是按范围逐槽失效: 环形缓冲按取模映射, 一个槽位可能同时对应
// 多个索引, 光凭 min/max 判断不出哪些槽该失效; 而删除在 raft 里是低频操作(只在
// 快照截断与冲突回退时发生), 全清换正确性划算。注意先清缓存再删底层: 反过来会
// 留下一段"底层已删、缓存还命中"的窗口。
func (c *LogCache) DeleteRange(min, max uint64) error {
	// Invalidate the cache on deletes
	c.l.Lock()
	c.cache = make([]*Log, len(c.cache))
	c.l.Unlock()

	return c.store.DeleteRange(min, max)
}
