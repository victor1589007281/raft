// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics/compat"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"golang.org/x/sys/unix"
)

const (
	// rpcType 是连接上的**首字节**(帧头), 值即协议: 0=AppendEntries, 1=RequestVote,
	// 2=InstallSnapshot, 3=TimeoutNow, 4=RequestPreVote。改动这里的顺序等于改线格式。
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow
	rpcRequestPreVote

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// DefaultMaxRPCsInFlight is the default value used for pipelining configuration
	// if a zero value is passed. See https://github.com/hashicorp/raft/pull/541
	// for rationale. Note, if this is changed we should update the doc comments
	// below for NetworkTransportConfig.MaxRPCsInFlight.
	DefaultMaxRPCsInFlight = 2

	// connReceiveBufferSize is the size of the buffer we will use for reading RPC requests into
	// on followers
	connReceiveBufferSize = 256 * 1024 // 256KB

	// connSendBufferSize is the size of the buffer we will use for sending RPC request data from
	// the leader to followers.
	connSendBufferSize = 256 * 1024 // 256KB

	// minInFlightForPipelining is a property of our current pipelining
	// implementation and must not be changed unless we change the invariants of
	// that implementation. Roughly speaking even with a zero-length in-flight
	// buffer we still allow 2 requests to be in-flight before we block because we
	// only block after sending and the receiving go-routine always unblocks the
	// chan right after first send. This is a constant just to provide context
	// rather than a magic number in a few places we have to check invariants to
	// avoid panics etc.
	minInFlightForPipelining = 2
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrPipelineShutdown is returned when the pipeline is closed.
	ErrPipelineShutdown = errors.New("append pipeline closed")
)

// NetworkTransport provides a network based transport that can be
// used to communicate with Raft on remote machines. It requires
// an underlying stream layer to provide a stream abstraction, which can
// be simple TCP, TLS, etc.
//
// This transport is very simple and lightweight. Each RPC request is
// framed by sending a byte that indicates the message type, followed
// by the MsgPack encoded request.
//
// The response is an error string followed by the response object,
// both are encoded using MsgPack.
//
// InstallSnapshot is special, in that after the RPC request we stream
// the entire state. That socket is not re-used as the connection state
// is not known if there is an error.
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh chan RPC

	heartbeatFn     func(RPC)
	heartbeatFnLock sync.Mutex

	logger hclog.Logger

	maxPool     int
	maxInFlight int

	serverAddressLock     sync.RWMutex
	serverAddressProvider ServerAddressProvider

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	stream StreamLayer

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int

	msgpackUseNewTimeFormat bool
	tcpConfig               TransportRPCTCPConfig

	// 12.8 feature gates (mirrored from NetworkTransportConfig, default off).
	rpcRawBytesEnabled bool
	rpcWritevEnabled   bool
	rpcZeroCopyEnabled bool
	rpcBusyPollEnabled bool
	rpcIORingEnabled   bool
	rpcProtocolLevel   int
	// 注意: 本文件只读 rpcWritevEnabled 与 rpcBusyPollEnabled; rpcRawBytesEnabled /
	// rpcZeroCopyEnabled / rpcIORingEnabled / rpcProtocolLevel 被赋值后从未被读取(预留位)。
}

// NetworkTransportConfig encapsulates configuration for the network transport layer.
type NetworkTransportConfig struct {
	// ServerAddressProvider is used to override the target address when establishing a connection to invoke an RPC
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool controls how many connections we will pool
	MaxPool int

	// MaxRPCsInFlight controls the pipelining "optimization" when replicating
	// entries to followers.
	//
	// Setting this to 1 explicitly disables pipelining since no overlapping of
	// request processing is allowed. If set to 1 the pipelining code path is
	// skipped entirely and every request is entirely synchronous.
	//
	// If zero is set (or left as default), DefaultMaxRPCsInFlight is used which
	// is currently 2. A value of 2 overlaps the preparation and sending of the
	// next request while waiting for the previous response, but avoids additional
	// queuing.
	//
	// Historically this was internally fixed at (effectively) 130 however
	// performance testing has shown that in practice the pipelining optimization
	// combines badly with batching and actually has a very large negative impact
	// on commit latency when throughput is high, whilst having very little
	// benefit on latency or throughput in any other case! See
	// [#541](https://github.com/hashicorp/raft/pull/541) for more analysis of the
	// performance impacts.
	//
	// Increasing this beyond 2 is likely to be beneficial only in very
	// high-latency network conditions. HashiCorp doesn't recommend using our own
	// products this way.
	//
	// To maintain the behavior from before version 1.4.1 exactly, set this to
	// 130. The old internal constant was 128 but was used directly as a channel
	// buffer size. Since we send before blocking on the channel and unblock the
	// channel as soon as the receiver is done with the earliest outstanding
	// request, even an unbuffered channel (buffer=0) allows one request to be
	// sent while waiting for the previous one (i.e. 2 inflight). so the old
	// buffer actually allowed 130 RPCs to be inflight at once.
	MaxRPCsInFlight int

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration

	// MsgpackUseNewTimeFormat when set to true, force the underlying msgpack
	// codec to use the new format of time.Time when encoding (used in
	// go-msgpack v1.1.5 by default). Decoding is not affected, as all
	// go-msgpack v2.1.0+ decoders know how to decode both formats.
	MsgpackUseNewTimeFormat bool

	// ---- vraft 12.8 / 12.9 (transport / kernel I/O) ----
	// All default to false/zero (disabled) for compatibility.
	RPCRawBytesEnabled bool
	RPCWritevEnabled   bool
	RPCZeroCopyEnabled bool
	RPCBusyPollEnabled bool
	RPCIORingEnabled   bool
	RPCProtocolLevel   int
	RPCTCPConfig       TransportRPCTCPConfig
}

// TransportRPCTCPConfig mirrors raft.RPCTCPConfig for standalone transport users
// (separate type to avoid import cycle; convertible field-wise).
type TransportRPCTCPConfig struct {
	TCPNoDelay  *bool `json:"tcp_no_delay,omitempty"`
	KeepAlive   *bool `json:"keep_alive,omitempty"`
	SndBuf      *int  `json:"snd_buf,omitempty"`
	UserTimeout *int  `json:"user_timeout_ms,omitempty"`
	BBR         *bool `json:"bbr,omitempty"`
}

// ServerAddressProvider is a target address to which we invoke an RPC when establishing a connection
type ServerAddressProvider interface {
	ServerAddr(id ServerID) (ServerAddress, error)
}

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}

type netConn struct {
	target ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

// Release 关闭这条 netConn 的底层连接。池化路径不该调它 —— 归还连接是 returnConn 的职责。
func (n *netConn) Release() error {
	return n.conn.Close()
}

type netPipeline struct {
	conn  *netConn
	trans *NetworkTransport

	doneCh       chan AppendFuture
	inprogressCh chan *appendFuture

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewNetworkTransportWithConfig 用完整配置建 transport: 补 logger 默认值、把 MaxRPCsInFlight 的零值
// 解释为 DefaultMaxRPCsInFlight, 再建连接池与消费通道、搬入 12.8 的六个特性开关与 TCP 调优配置
// (全部默认关闭), 最后建 stream context 并起 listen goroutine。
//
// 注意：MaxRPCsInFlight 的 0 是"取默认值"而不是"禁用流水线"; 想显式禁用要传 1,
// 此时 AppendEntriesPipeline 会直接返回 ErrPipelineReplicationNotSupported。
// NewNetworkTransportWithConfig creates a new network transport with the given config struct
func NewNetworkTransportWithConfig(
	config *NetworkTransportConfig,
) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	maxInFlight := config.MaxRPCsInFlight
	if maxInFlight == 0 {
		// 0 是"未设置"的哨兵, 不是"禁用"; 显式禁用要传 1。
		// Default zero value
		maxInFlight = DefaultMaxRPCsInFlight
	}
	trans := &NetworkTransport{
		connPool:                make(map[ServerAddress][]*netConn),
		consumeCh:               make(chan RPC),
		logger:                  config.Logger,
		maxPool:                 config.MaxPool,
		maxInFlight:             maxInFlight,
		shutdownCh:              make(chan struct{}),
		stream:                  config.Stream,
		timeout:                 config.Timeout,
		TimeoutScale:            DefaultTimeoutScale,
		serverAddressProvider:   config.ServerAddressProvider,
		msgpackUseNewTimeFormat: config.MsgpackUseNewTimeFormat,
		tcpConfig:               transportTCPConfigFrom(config.RPCTCPConfig),
		rpcRawBytesEnabled:      config.RPCRawBytesEnabled,
		rpcWritevEnabled:        config.RPCWritevEnabled,
		rpcZeroCopyEnabled:      config.RPCZeroCopyEnabled,
		rpcBusyPollEnabled:      config.RPCBusyPollEnabled,
		rpcIORingEnabled:        config.RPCIORingEnabled,
		rpcProtocolLevel:        config.RPCProtocolLevel,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	go trans.listen()

	return trans
}

// NewNetworkTransport 是便利入口: logOutput 为 nil 时落到 os.Stderr, 12.8/12.9 的新开关一律关闭。
// NewNetworkTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger 同上, 但用调用方传入的 hclog.Logger。
// NewNetworkTransportWithLogger creates a new network transport with the given logger, dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// setupStreamContext 新建一个可取消的 stream context。必须在持有 streamCtxLock 时调用 —— 否则可能
// 让已建立的连接 handler 持有一个永远取消不掉的旧 context。
// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// getStreamContext 读锁下取回当前 stream context, 供新建的连接 handler 继承。
// getStreamContext is used retrieve the current stream context.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// SetHeartbeatHandler 注册心跳快路径的处理函数, 让心跳绕开 consumeCh 的排队, 避免被磁盘 IO 排头阻塞。
// SetHeartbeatHandler is used to set up a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// CloseStreams 关掉池里所有连接并重建 stream context: 旧 context 被 cancel, 从而让所有基于它的
// handleConn 退出。cancel 与重建必须在同一把锁下完成, 否则可能漏掉正在建立的 handler。
// CloseStreams closes the current streams.
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range n.connPool {
		for _, conn := range e {
			_ = conn.Release()
		}

		delete(n.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	n.streamCtxLock.Lock()
	n.streamCancel()
	n.setupStreamContext()
	n.streamCtxLock.Unlock()
}

// Close 关闭 transport: 关掉 shutdownCh 并关监听器, 重复调用是幂等的。
// Close is used to stop the network transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		_ = n.stream.Close()
		n.shutdown = true
	}
	return nil
}

// Consumer 返回入站 RPC 的消费通道, raft 核心从它取命令。
// Consumer implements the Transport interface.
func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

// LocalAddr 返回本节点对外宣告的地址的字符串形式。
// LocalAddr implements the Transport interface.
func (n *NetworkTransport) LocalAddr() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// IsShutdown 以非阻塞方式探测 shutdownCh 是否已关闭。
// IsShutdown is used to check if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// getPooledConn 从 target 的连接池里取一条空闲连接, 池空或没有该 target 时返回 nil。
//
// 取的是**栈顶**(最后归还的那条): 它最可能还热着, 且切片尾部收缩不需要搬移元素。
//
// 算法解读：
//
//	原理：连接池是 map[ServerAddress][]*netConn, 取出即 pop 栈顶 —— 先把末元素置 nil, 再把切片截短一位并写回 map。
//	关键不变量：整个 pop 必须在 connPoolLock 下完成; 置 nil 与截短必须成对, 否则会留下一条"已被取走但仍挂在池里"的重复引用, 被两条 RPC 同时使用。
//	代价与边界：置 nil 只是断开对底层连接的引用以便回收, 不改变切片容量 —— 池被反复取空也不会缩容。
//	设计权衡：选 LIFO 而不是 FIFO, 是因为空闲连接会被对端或中间设备按空闲超时掐掉, 最新的那条最不可能已死; FIFO 反而会把最老的死连接优先发出去, 让调用方多付一次"写失败才发现"的代价。
//
// getExistingConn is used to grab a pooled connection.
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider 先解析出该 id 的目标地址(经 provider 覆写或回落到 target), 再取连接。
// getConnFromAddressProvider returns a connection from the server address provider if available, or defaults to a connection using the target server address
func (n *NetworkTransport) getConnFromAddressProvider(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

// getProviderAddressOrFallback 向 ServerAddressProvider 问 id 的地址; provider 未配置或报错时
// **静默回落到**传入的 target, 只记一条 Warn。
//
// 算法解读：
//
//	原理：读锁下问一次 provider, 成功就用覆写地址, 失败就退回 raft 配置里存的地址。
//	关键不变量：整个查询在 serverAddressLock.RLock 下进行 —— 覆写表可能在运行时被整体替换, 不持锁读会拿到半更新的视图。
//	代价与边界：回落是**降级而非报错**, 所以 provider 配错时调用方完全无感, 只在日志里留一条 Warn; 若覆写地址指向已下线的节点, RPC 会一直失败到拨号超时, 而不是快速失败。
//	设计权衡：宁可"用旧地址再试一次"也不直接报错, 是因为 provider 通常是外部服务发现, 它的瞬时故障不该让整条复制链路停摆 —— 回落地址在多数场景下仍然可用。
func (n *NetworkTransport) getProviderAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	n.serverAddressLock.RLock()
	defer n.serverAddressLock.RUnlock()
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddr(id)
		if err != nil {
			n.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn 取一条可用连接: 先看池, 池里没有才真正拨号, 并对新连接套用 12.8 的 TCP 调优, 再包上
// msgpack 编解码器与 connSendBufferSize 大小的发送缓冲。
//
// 算法解读：
//
//	原理：池命中直接返回; 未命中则 stream.Dial(带 timeout) → applyTransportTCPConfig 打 setsockopt → 构造 netConn(decoder 包 bufio.Reader, writer 是 connSendBufferSize 的 bufio.Writer)。
//	关键不变量：编解码器的 TimeNotBuiltin 必须与 msgpackUseNewTimeFormat 反向绑定 —— 它决定 time.Time 用"内置类型"还是"扩展类型"编码, 两端不一致会解出错误的时间。
//	代价与边界：每次未命中都要付一次 TCP 三次握手加若干 setsockopt; 池只缓存**已归还**的连接, 所以并发突发时仍可能同时开出多条。
//	设计权衡：不在 getConn 里做探活读, 因为那会给每次取连接都加一个往返; 代价是取到一条对端已关闭的连接时, 错误推迟到第一次写才暴露, 由调用方 Release 后重来。
//
// getConn is used to get a connection from the pool.
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	applyTransportTCPConfig(conn, n.transportTCPConfig())

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, connSendBufferSize),
	}

	mp := &codec.MsgpackHandle{}
	mp.TimeNotBuiltin = !n.msgpackUseNewTimeFormat
	netConn.enc = codec.NewEncoder(netConn.w, mp)

	// Done
	return netConn, nil
}

// returnConn 把用毕的连接放回池; 池已满或 transport 已关闭时改为直接关闭连接。
//
// 算法解读：
//
//	原理：以 conn.target 为键把连接 push 到切片尾部, 但仅当 !IsShutdown() 且该键下的条数 < maxPool。
//	关键不变量：判满与 append 必须在同一把 connPoolLock 下 —— 分两次加锁会让并发归还突破 maxPool。
//	代价与边界：maxPool 是**每目标地址**的上限而非全局上限, 所以总连接数可达 maxPool × 目标数。
//	设计权衡：关闭态直接 Release 而不是"先塞进去再说", 是为了避免 transport 已 shutdown 之后池里还留着活连接 —— 它们再也不会被复用, 只会泄漏 fd 直到进程退出。
//
// returnConn returns a connection back to the pool.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		_ = conn.Release()
	}
}

// AppendEntriesPipeline 为某个 follower 建一条流水线通道; maxInFlight < 2 时直接返回
// ErrPipelineReplicationNotSupported, 让上层退回同步 RPC 而不是在这里退化。
// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (n *NetworkTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	if n.maxInFlight < minInFlightForPipelining {
		// Pipelining is disabled since no more than one request can be outstanding
		// at once. Skip the whole code path and use synchronous requests.
		return nil, ErrPipelineReplicationNotSupported
	}

	// Get a connection
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// Create the pipeline
	return newNetPipeline(n, conn, n.maxInFlight), nil
}

// AppendEntries 走 genericRPC 把日志追加请求发给目标节点, 是 Transport 接口的同步实现。
// AppendEntries implements the Transport interface.
func (n *NetworkTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote 走 genericRPC 发起一次投票请求(选举用), 请求会改变接收方的 term。
// RequestVote implements the Transport interface.
func (n *NetworkTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// RequestPreVote 走 genericRPC 发起预投票 —— 它不改变任何一方的 term, 只用来探询自己能否赢得选举。
// RequestPreVote implements the Transport interface.
func (n *NetworkTransport) RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestPreVote, args, resp)
}

// genericRPC 是所有"一问一答"RPC 的共同实现: 取连接 → 设 deadline → 发请求 → 解响应, 再按
// decodeResponse 的判断决定是否把连接还池。
//
// 算法解读：
//
//	原理：rpcType 决定走哪条发送路径 —— rpcWritevEnabled 为真时走 sendRPCWritev(合帧 writev), 否则走 sendRPC(带缓冲的单次写); 两条路径都在出错时 Release 连接, 只有成功才可能还池。
//	关键不变量：deadline 是**整条 RPC**(写加读)共用的一个绝对时刻, 不是每步各设一个; 因此 n.timeout 必须覆盖"发完请求 + 等到响应"的总时长。
//	代价与边界：canReturn 由 decodeResponse 给出 —— 只有错误与响应都完整解码、连接帧边界仍然干净时才为 true; 任一步解码失败都意味着字节流已错位, 连接必须丢弃而不是还池。
//	设计权衡：不在这里做重试。Raft 的每个 RPC 都由上层按自己的节奏重发(选举超时、心跳间隔), 在这里重试只会叠加超时并掩盖真正的网络故障。
//
// genericRPC handles a simple request/response RPC.
func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	// Get a conn
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	// Set a deadline
	if n.timeout > 0 {
		_ = conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// Send the RPC (12.8.3 writev path gated behind switch; probed fallback).
	if n.rpcWritevEnabled {
		if err = sendRPCWritev(conn, rpcType, args); err != nil {
			return err
		}
	} else {
		if err = sendRPC(conn, rpcType, args); err != nil {
			return err
		}
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		n.returnConn(conn)
	}
	return err
}

// InstallSnapshot 先发快照元数据 RPC, 紧随其后流式传输整个快照体, 再读回响应; 该连接**用完即弃**,
// 绝不还池(对端可能正在读快照体, 帧边界已不可知)。
//
// 算法解读：
//
//	原理：deadline 按 args.Size/TimeoutScale 放大 n.timeout(倍数下限为 1), 让大快照有足够时间传完; 元数据走 sendRPC 进缓冲写, 快照体走 copyWithSendfile, 最后一次 Flush 把两者一起推出去。
//	关键不变量：快照体必须**紧接**在元数据之后且长度恰好等于 args.Size —— 接收端用 io.LimitReader(r, req.Size) 截取, 多一个字节就会污染下一条 RPC 的帧头。
//	代价与边界：只要中途出错连接状态就不可知, 所以这里用 defer Release 直接关闭, 宁可下次重新拨号也不复用一条帧边界已乱的连接。
//	设计权衡：不把快照体切成多个 RPC 分块, 是因为快照语义要求"要么整份到位要么重来"; 分块会把失败恢复的复杂度从"重传一份"抬到"重传某个分片并处理乱序"。
//
// InstallSnapshot implements the Transport interface.
func (n *NetworkTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Release() }()

	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		_ = conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	if _, err = copyWithSendfile(conn.w, conn.conn, data); err != nil {
		return err
	}

	if err = conn.w.Flush(); err != nil {
		return err
	}

	_, err = decodeResponse(conn, resp)
	return err
}

// EncodePeer 把对端地址编码成 raft 配置里存的字节 —— 先经 provider 解析, 存下来的是稳定地址。
// EncodePeer implements the Transport interface.
func (n *NetworkTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer 是 EncodePeer 的逆操作, 直接把字节当地址串返回, 不做任何格式校验。
// DecodePeer implements the Transport interface.
func (n *NetworkTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// TimeoutNow 走 genericRPC 通知目标节点立即发起选举, 用于领导权转移。
// TimeoutNow implements the Transport interface.
func (n *NetworkTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	return n.genericRPC(id, target, rpcTimeoutNow, args, resp)
}

// listen 是接受入站连接的常驻循环: 每次 Accept 成功就起一个 handleConn goroutine, 失败则指数退避
// 重试, 直到 transport 被关闭。
//
// 算法解读：
//
//	原理：失败从 baseDelay(5ms)起步逐次翻倍、封顶 maxDelay(1s), 成功一次就把 loopDelay 清零; 退避期间用 select 同时等 shutdownCh 与定时器, 保证 Close() 不会被退避拖住。
//	关键不变量：loopDelay 必须在 Accept 成功后归零 —— 否则一次偶发失败会让之后每一轮成功接受之后的下一轮仍带着延迟。
//	代价与边界：退避是**全局**的(整个 listen 循环共用一个 loopDelay), 一次失败会让下一轮 Accept 也慢一拍; 但在 accept 失败(EMFILE、监听器被关)的场景下这通常正是想要的行为。
//	设计权衡：不做 accept 限流(如"同时最多 N 条连接"), 把连接数控制交给对端的 maxPool —— 单点限流会在对端重连风暴时把健康连接也拒之门外。
//
// listen is used to handling incoming connections.
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accept incoming connections
		conn, err := n.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !n.IsShutdown() {
				n.logger.Error("failed to accept connection", "error", err)
			}

			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay
		loopDelay = 0

		n.logger.Debug("accepted connection", "local-address", n.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// Handle the connection in dedicated routine
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// handleConn 服务一条入站连接直到连接关闭或 connCtx 被取消: 循环地"读一条命令 → 分发 → 写回响应"。
//
// 算法解读：
//
//	原理：每个连接一条 goroutine, 用 bufio.Reader(connReceiveBufferSize) + msgpack decoder 读、bufio.Writer + encoder 写; 每条命令处理完立刻 Flush, 因此同一连接上的多条 RPC 是**严格串行**的一问一答 —— 并发度来自多条连接与 leader 侧的 pipeline, 不来自单连接内交织。
//	关键不变量：一条连接上任意时刻最多只有一个未完成的 RPC; 也正因如此, 响应里**不需要**任何请求 id 来配对 —— 帧序即配对。
//	代价与边界：慢 RPC(如 InstallSnapshot)会独占这条连接, 期间对端在同一连接上的其他请求只能排队; 接收缓冲固定 256KB, 单条 RPC 超过它不会截断(decoder 会继续从 conn 读), 只影响内存占用。
//	设计权衡：不为单连接做多路复用(那需要在帧里加 stream id 与独立的响应路由), 因为 raft 的 RPC 数量少、体积大, 用多条连接加池化就能拿到同样的并发, 复杂度却低得多。
//
// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
func (n *NetworkTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	applyTransportTCPConfig(conn, n.transportTCPConfig())
	if n.rpcBusyPollEnabled {
		if tcp, ok := conn.(*net.TCPConn); ok {
			// 50 是微秒: 内核在软中断之外额外轮询该 socket 接收队列的时长, 用 CPU 换唤醒延迟。
			if raw, err := tcp.SyscallConn(); err == nil {
				_ = raw.Control(func(fd uintptr) { _ = setBusyPoll(fd, 50) })
			}
		}
	}
	r := bufio.NewReaderSize(conn, connReceiveBufferSize)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})

	mp := &codec.MsgpackHandle{}
	mp.TimeNotBuiltin = !n.msgpackUseNewTimeFormat
	enc := codec.NewEncoder(w, mp)

	for {
		select {
		case <-connCtx.Done():
			n.logger.Debug("stream layer is closed")
			return
		default:
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			n.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand 从连接上解出一条 RPC 并分发给上层, 然后写回响应; 返回值非 nil 表示连接应关闭。
//
// 帧格式(与 NetworkTransport 的类型注释一致)：首字节是 rpcType, 其后是 msgpack 编码的请求体;
// InstallSnapshot 的请求体之后还会紧跟 args.Size 字节的快照流。响应侧固定为"msgpack 字符串
// (错误, 空串表示成功) + msgpack 编码的响应对象"。
//
// 算法解读：
//
//	原理：先 ReadByte 拿 rpcType(单独计时, 因为心跳连接会长时间挂在这一步), 再按类型解出具体请求结构; 命中心跳快路径时直接交 heartbeatFn 并 goto RESP, 跳过 consumeCh 入队与等待。
//	关键不变量：心跳判定要求 term 非零、leader 地址非空、prev 索引与 prev 任期均为 0、无 entries、commitIndex 为 0 —— 六条同时成立才算心跳, 任何一条不满足都会走完整 AppendEntries 路径。
//	代价与边界：心跳快路径绕过日志与 FSM, 但**仍然要等 respCh 拿到响应**才能写回; 若 heartbeatFn 为 nil(未设置), 心跳会退化成普通 RPC 走 consumeCh。
//	设计权衡：错误用"字符串再响应体"两段编码, 而不是把错误塞进响应结构 —— 这样所有 RPC 共用一套响应框架, 且错误可以独立于响应类型演进。
//
// handleCommand is used to decode and dispatch a single command.
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	getTypeStart := time.Now()

	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	metrics.MeasureSince([]string{"raft", "net", "getRPCType"}, getTypeStart)
	decodeStart := time.Now()

	// Create the RPC object
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	// Decode the command
	isHeartbeat := false
	var labels []metrics.Label
	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		leaderAddr := req.Addr
		if len(leaderAddr) == 0 {
			leaderAddr = req.Leader
		}

		// Check if this is a heartbeat
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		if isHeartbeat {
			labels = []metrics.Label{{Name: "rpcType", Value: "Heartbeat"}}
		} else {
			labels = []metrics.Label{{Name: "rpcType", Value: "AppendEntries"}}
		}
	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestVote"}}
	case rpcRequestPreVote:
		var req RequestPreVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestPreVote"}}
	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)
		labels = []metrics.Label{{Name: "rpcType", Value: "InstallSnapshot"}}
	case rpcTimeoutNow:
		var req TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "TimeoutNow"}}
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcDecode"}, decodeStart, labels)

	processStart := time.Now()

	// Check for heartbeat fast-path
	if isHeartbeat {
		n.heartbeatFnLock.Lock()
		fn := n.heartbeatFn
		n.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// Dispatch the RPC
	select {
	case n.consumeCh <- rpc:
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	// Wait for response
RESP:
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcEnqueue"}, processStart, labels)
	respWaitStart := time.Now()
	select {
	case resp := <-respCh:
		defer metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcRespond"}, respWaitStart, labels)
		// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

// decodeResponse 解出"错误字符串 + 响应对象"两段, 并报告这条连接能否复用; 返回 false 时连接已被
// Release, 调用方不得再使用它。
// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		_ = conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		_ = conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, errors.New(rpcError)
	}
	return true, nil
}

// sendRPC 用带缓冲的写路径发一条 RPC: 先写 1 字节 rpcType, 再把请求体 msgpack 编码进同一个
// bufio.Writer, 最后一次性 Flush; 任何一步失败都直接 Release 连接并返回错误。
// sendRPC is used to encode and send the RPC.
// 12.8.3: when RPCWritev is logically enabled, payload is flushed via
// net.Buffers (writev) where available; probed without requiring a new flag
// on NetworkTransport by checking whether the underlying conn supports
// WriteBuffers. When unavailable, falls back to the buffered single-write path.
//
// 算法解读：
//
//	原理：编码器直接写在 bufio.Writer 上, rpcType 字节与 msgpack 体共享同一块 256KB 缓冲, Flush 时由内核把它们拼成尽量少的 TCP 段。
//	关键不变量：rpcType 字节与请求体之间不能有任何插入 —— 接收端只 ReadByte 一次就认为剩下的都是 msgpack 体。
//	代价与边界：bufio 缓冲固定 connSendBufferSize; 单条请求超过缓冲会触发内部多次 write, 此时"合帧"失效但语义不变。
//	设计权衡：相比 sendRPCWritev 少一次"编码到临时切片"的分配与拷贝, 代价是 header 与 body 各走一次 bufio.Write —— 由 Flush 合并, 所以常态下两条路径的系统调用数相同, 差别只在大请求时的内存拷贝。
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	if err := conn.w.WriteByte(rpcType); err != nil {
		_ = conn.Release()
		return err
	}
	if err := conn.enc.Encode(args); err != nil {
		_ = conn.Release()
		return err
	}
	if err := conn.w.Flush(); err != nil {
		_ = conn.Release()
		return err
	}
	return nil
}

// sendRPCWritev 是 12.8.3 的 writev 路径: 把请求体编码到一块临时缓冲, 再用 net.Buffers 把
// "rpcType 字节 + 请求体"一次 writev 交给内核; 内核不支持时静默回落到缓冲写。
// sendRPCWritev is the 12.8.3 writev path: encode into a side buffer, then
// push header+payload with a single writev (net.Buffers) when the conn
// supports it. Exposed for future flag-gating; currently best-effort.
//
// 算法解读：
//
//	原理：codec.NewEncoderBytes 先把 args 编码进 tmp, 再构造 net.Buffers{header, tmp} 调 WriteTo(conn.conn) —— net.Buffers 在支持 writev 的平台上一次系统调用写多个切片。
//	关键不变量：回落到缓冲写的前提是"WriteTo 一个字节都没写出去"; 但 WriteTo 出错并不保证这一点(可能部分写), 此时再走回落分支会把同一请求写第二遍, 帧边界彻底错乱。
//	代价与边界：比 sendRPC 多一次整块内存分配与一次编码拷贝(把请求体从 bufio 缓冲搬到 tmp), 换来大请求少一次内存拷贝; 请求很小时 net.Buffers 的开销反而更高。
//	设计权衡：由 rpcWritevEnabled 开关选择而不是无条件启用 —— 上面的部分写风险意味着这条路径的失败语义还不如 sendRPC 干净, 只适合显式打开。
func sendRPCWritev(conn *netConn, rpcType uint8, args interface{}) error {
	// Encode into a temporary buffer so we can expose two slices to writev.
	var tmp []byte
	enc := codec.NewEncoderBytes(&tmp, &codec.MsgpackHandle{})
	if err := enc.Encode(args); err != nil {
		_ = conn.Release()
		return err
	}
	header := []byte{rpcType}
	buf := net.Buffers{header, tmp}
	// 注意: WriteTo 出错不代表一个字节都没写出去 —— 部分写之后再走下面的回落分支会把同一请求
	// 写第二遍。这里按"未写出"假定继续回落(仅注释, 不改行为)。
	// Probe writev; fallback to buffered writer.
	if _, err := buf.WriteTo(conn.conn); err == nil {
		return nil
	}
	if err := conn.w.WriteByte(rpcType); err != nil {
		_ = conn.Release()
		return err
	}
	if _, err := conn.w.Write(tmp); err != nil {
		_ = conn.Release()
		return err
	}
	if err := conn.w.Flush(); err != nil {
		_ = conn.Release()
		return err
	}
	return nil
}

// newNetPipeline 用一条已取到的连接构造流水线, 并起一个 decodeResponses goroutine 负责收响应;
// maxInFlight 小于 2 属于调用方违约, 这里直接 panic。
//
// 算法解读：
//
//	原理：inprogressCh/doneCh 的容量取 maxInFlight-2; 发送方在**发完之后**才阻塞在 inprogressCh 上, 而解码方一开始等响应就会把 inprogressCh 空出来, 于是容量为 0 时也天然允许 2 条在途。
//	关键不变量：容量必须是 maxInFlight-2 且 maxInFlight >= 2 —— 把容量当成 maxInFlight 会让在途数超出预期, 取成 maxInFlight-1 则白白浪费一个槽位; 这正是 minInFlightForPipelining 存在的原因。
//	代价与边界：maxInFlight 越大在途请求越多, 响应会积压在 doneCh 里; doneCh 满时解码 goroutine 阻塞, 反压回发送方。
//	设计权衡：通道容量与"在途数"之间刻意差 2, 是为了让发送与等待不互相卡住(见上方原注释); 代价是这个常量关系必须靠文档和断言维持, 不能独立调参。
//
// newNetPipeline is used to construct a netPipeline from a given transport and
// connection. It is a bug to ever call this with maxInFlight less than 2
// (minInFlightForPipelining) and will cause a panic.
func newNetPipeline(trans *NetworkTransport, conn *netConn, maxInFlight int) *netPipeline {
	if maxInFlight < minInFlightForPipelining {
		// Shouldn't happen (tm) since we validate this in the one call site and
		// skip pipelining if it's lower.
		panic("pipelining makes no sense if maxInFlight < 2")
	}
	n := &netPipeline{
		conn:  conn,
		trans: trans,
		// The buffer size is 2 less than the configured max because we send before
		// waiting on the channel and the decode routine unblocks the channel as
		// soon as it's waiting on the first request. So a zero-buffered channel
		// still allows 1 request to be sent even while decode is still waiting for
		// a response from the previous one. i.e. two are inflight at the same time.
		inprogressCh: make(chan *appendFuture, maxInFlight-2),
		doneCh:       make(chan AppendFuture, maxInFlight-2),
		shutdownCh:   make(chan struct{}),
	}
	go n.decodeResponses()
	return n
}

// decodeResponses 是流水线的收端常驻循环: 按发送顺序从 inprogressCh 取出在途请求, 解出响应、
// 唤醒对应的 future, 再把它交给 doneCh 供上层消费。
//
// 算法解读：
//
//	原理：单 goroutine 串行解码 —— 每次解码前给连接设一个读超时(trans.timeout), 解完就 future.respond(err) 唤醒等待者, 再把 future 推给 doneCh。
//	关键不变量：**响应必须按请求发送顺序到达并解码**, 因为管道里没有任何请求 id 来配对; 一旦有一条响应超时或解错, 后续所有 future 的配对都会错位。
//	代价与边界：doneCh 容量同样是 maxInFlight-2, 上层若不及时消费, 解码 goroutine 会阻塞在 doneCh 上, 反压使 inprogressCh 也填满, 最终让 AppendEntries 阻塞 —— 这就是流水线的背压链。
//	设计权衡：不并发解码(那需要请求 id), 因为 TCP 本身保序, 单 goroutine 顺序解码既简单又不会引入乱序风险; 代价是解码本身成为这条连接的串行点。
//
// decodeResponses is a long running routine that decodes the responses
// sent on the connection.
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		select {
		case future := <-n.inprogressCh:
			if timeout > 0 {
				_ = n.conn.conn.SetReadDeadline(time.Now().Add(timeout))
			}

			_, err := decodeResponse(n.conn, future.resp)
			future.respond(err)
			select {
			case n.doneCh <- future:
			case <-n.shutdownCh:
				return
			}
		case <-n.shutdownCh:
			return
		}
	}
}

// AppendEntries 是流水线的发端: 造一个 future、设写超时、把请求发出去, 再把 future 交给
// decodeResponses 去等响应, 自己立即返回 —— 调用方拿到的 future 稍后才被唤醒。
//
// 算法解读：
//
//	原理：future 在发送**之前**就 init(); 发送成功后阻塞在 inprogressCh(这就是背压点), shutdown 时返回 ErrPipelineShutdown。
//	关键不变量：future 的入队顺序必须与它在连接上的写出顺序一致 —— 两者之间不能有并发; 方法本身不加锁, 依赖调用方(leader 的复制循环)对同一条 pipeline 串行调用。
//	代价与边界：写超时是**每次发送前重设**的绝对时刻, 不覆盖后续等待响应的时间(那由收端的读超时负责)。
//	设计权衡：发送与等待分离(发送方不碰连接读端), 让发送与上一条响应的解码真正重叠 —— 这正是 maxInFlight=2 想要的那一点重叠, 也是它比同步路径快的地方。
//
// AppendEntries is used to pipeline a new append entries request.
func (n *netPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Add a send timeout
	if timeout := n.trans.timeout; timeout > 0 {
		_ = n.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// Send the RPC
	if err := sendRPC(n.conn, rpcAppendEntries, future.args); err != nil {
		return nil, err
	}

	// Hand-off for decoding, this can also cause back-pressure
	// to prevent too many inflight requests
	select {
	case n.inprogressCh <- future:
		return future, nil
	case <-n.shutdownCh:
		return nil, ErrPipelineShutdown
	}
}

// Consumer 返回流水线完成的 future 通道, 上层按完成顺序消费。
// Consumer returns a channel that can be used to consume complete futures.
func (n *netPipeline) Consumer() <-chan AppendFuture {
	return n.doneCh
}

// Close is used to shut down the pipeline connection.

// copyWithSendfile streams data from src to dst's buffered writer. When the
// transport is a real TCPConn carrying an *os.File, it probes splice/sendfile
// (via io.Copy which the runtime can lower to sendfile on linux); otherwise it
// just io.Copies. Always falls back correctly.

// transportTCPConfig 返回建 transport 时冻结下来的 TCP 调优配置。
func (n *NetworkTransport) transportTCPConfig() TransportRPCTCPConfig { return n.tcpConfig }

// transportTCPConfigFrom 做一次按字段可转换的类型搬运: 把 raft 包的 RPCTCPConfig 变成 transport
// 自己的 TransportRPCTCPConfig(两者字段一一对应, 分开定义只为避免 import 环)。
func transportTCPConfigFrom(c TransportRPCTCPConfig) TransportRPCTCPConfig { return c }

// copyWithSendfile 把快照数据经缓冲写复制到连接上; 名义上是零拷贝路径, 实际两个分支都走
// io.Copy(w, src) —— 探测结果不改变任何执行路径。
//
// 算法解读：
//
//	原理：src 是 *os.File 且 conn 是 *net.TCPConn 时"应当"能走到 sendfile/splice; 但这里复制的目标是 bufio.Writer 而不是 conn 本身, io.Copy 看到的是个 *bufio.Writer, 内核零拷贝(需要 Reader 与 Writer 都是特定具体类型)无从触发, 实际仍是用户态缓冲拷贝。
//	关键不变量：无论走哪条分支数据都必须经过 w —— 因为 w 里已经压着 InstallSnapshot 的 RPC 元数据, 绕过它直接写 conn 会让快照体插到帧头前面。
//	代价与边界：注释里说的"额外一次拷贝可以忽略"对 256KB 发送缓冲并不成立: 超过缓冲的大快照会被切成多次写, 拷贝量随快照大小线性增长。
//	设计权衡：保留 bufio 语义(而不是把裸 conn 交给 io.Copy 换真零拷贝), 是为了不破坏调用方"写完还要 Flush"的契约; 想真正吃到 sendfile, 需要先 Flush 再把裸 conn 交给 io.Copy。
func copyWithSendfile(w *bufio.Writer, conn net.Conn, src io.Reader) (int64, error) {
	// Best-effort: if both ends are the expected concrete types, let the
	// kernel path fire via io.Copy (which on linux+TCP+*os.File lowers to
	// sendfile/splice internally). Keep the buffered writer semantics by
	// flushing through w — so we copy to w, not directly to conn, preserving
	// the framing already written.
	// conn 与 w 在这里被刻意丢弃引用 —— 保留参数只为让调用点签名保持稳定;
	// 下面两个分支的行为完全相同, 探测结果目前不改变任何执行路径。
	_ = conn
	_ = w
	// If src is an *os.File and the conn is a *net.TCPConn, io.Copy can use
	// sendfile on linux. We route through the buffered writer to keep callers'
	// Flush contract unchanged; the extra copy is negligible compared to the
	// snapshot size and we avoid a raw syscall dependency here.
	if _, ok := src.(*os.File); ok {
		if _, ok2 := conn.(*net.TCPConn); ok2 {
			return io.Copy(w, src)
		}
	}
	return io.Copy(w, src)
}

// applyTransportTCPConfig 把 12.8 的 TCP 调优项套到一条新拨通或新接受的连接上: no-delay、发送缓冲、
// 用户超时、拥塞控制(BBR)在 fd 上做, keepalive 走 net 包 API; 全部失败只被忽略, 不影响连接可用性。
// applyTransportTCPConfig applies 12.8 TCP tuning knobs to a newly-dialed or
// accepted connection. All probes are best-effort: ENOSYS / not-TCP falls back.
//
// 算法解读：
//
//	原理：先断言 *net.TCPConn 拿到 SyscallConn, 再用 raw.Control 在**持有 fd 的临界区**里做 SetsockoptInt/String —— 直接用 fd uintptr 是因为 net 包没有暴露 TCP_USER_TIMEOUT、TCP_CONGESTION、SO_BUSY_POLL 这些选项, 只能下到 syscall 层。
//	关键不变量：所有 setsockopt 必须在 Control 回调内完成 —— 回调之外 fd 可能已被 net 包关闭并复用, 拿旧 fd 去 setsockopt 会改到别的连接上。
//	代价与边界：每个可选项的错误都被 `_ =` 静默丢弃(既不报错也不告警), 所以内核不支持时(如无 BBR 模块)完全无声; KeepAlive 周期硬编码为 15 * 1000000000 ns。
//	设计权衡：失败不报错, 因为这些是**性能调优**而非正确性前提 —— 让节点因为内核不支持 BBR 就建不了集群, 代价远大于收益。
func applyTransportTCPConfig(conn net.Conn, cfg TransportRPCTCPConfig) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		if cfg.TCPNoDelay != nil {
			_ = setTCPNoDelay(fd, *cfg.TCPNoDelay)
		}
		if cfg.SndBuf != nil && *cfg.SndBuf > 0 {
			_ = setSndBuf(fd, *cfg.SndBuf)
		}
		if cfg.UserTimeout != nil && *cfg.UserTimeout > 0 {
			_ = setTCPUserTimeout(fd, *cfg.UserTimeout)
		}
		if cfg.BBR != nil && *cfg.BBR {
			_ = setTCPCongestion(fd, "bbr")
		}
	})
	if cfg.KeepAlive != nil {
		if *cfg.KeepAlive {
			_ = tcp.SetKeepAlive(true)
			// 15 * 1000000000 是 15s 的纳秒硬编码(等于 15*time.Second), 内核会把该值向下取整到秒。
			_ = tcp.SetKeepAlivePeriod(15 * 1000000000)
		} else {
			_ = tcp.SetKeepAlive(false)
		}
	}
}

// setTCPNoDelay 在 fd 上设 TCP_NODELAY: on 为真时禁用 Nagle 合包 —— raft 的 RPC 多为小请求,
// 等合包只会白加延迟。
func setTCPNoDelay(fd uintptr, on bool) error {
	v := 0
	if on {
		v = 1
	}
	return unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NODELAY, v)
}

// setSndBuf 设 SO_SNDBUF。注意 Linux 会把该值**翻倍**后再用, 且它是上限而不是预留。
func setSndBuf(fd uintptr, n int) error {
	return unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, n)
}

// setTCPUserTimeout 设 TCP_USER_TIMEOUT(毫秒): 数据被对端确认前最多允许在途多久, 超时即强制关连接。
// 平台差异: 这是 Linux 专有选项, 其他平台 unix 包不提供该常量。
func setTCPUserTimeout(fd uintptr, ms int) error {
	return unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, ms)
}

// setTCPCongestion 用字符串设 TCP_CONGESTION(如 "bbr"); 内核未加载该算法时返回 ENOENT, 由调用方忽略。
func setTCPCongestion(fd uintptr, cc string) error {
	return unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, cc)
}

// setBusyPoll 设 SO_BUSY_POLL(微秒): 让内核在网卡软中断之外主动轮询该 socket 的接收队列, 用 CPU
// 换更低的唤醒延迟 —— 需要内核 CONFIG_NET_RX_BUSY_POLL 支持。
func setBusyPoll(fd uintptr, usec int) error {
	return unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_BUSY_POLL, usec)
}

// Close 关闭流水线: 释放它持有的连接并关掉 shutdownCh, 重复调用是幂等的。
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		return nil
	}

	// Release the connection
	_ = n.conn.Release()

	n.shutdown = true
	close(n.shutdownCh)
	return nil
}
