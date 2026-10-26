// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/hashicorp/go-hclog"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// HostnameAddr is a net.Addr whose String() is a "host:port" pair where host is
// a DNS name rather than an IP literal. A raft node can advertise a stable name
// (e.g. a Kubernetes StatefulSet headless-service FQDN such as "vraft-0.vraft")
// so that peer addresses stored in the raft configuration survive pod
// re-creation — a pod IP goes stale the moment the pod is rescheduled, and the
// leader would otherwise never recontact the recreated peer.
type HostnameAddr struct {
	network string
	addr    string
}

// NewHostnameAddr 用 "host:port" 串构造 HostnameAddr, network 固定为 "tcp"。
// NewHostnameAddr returns a HostnameAddr for the given "host:port" string.
func NewHostnameAddr(addr string) *HostnameAddr {
	return &HostnameAddr{network: "tcp", addr: addr}
}

// Network 返回该地址的网络类型, 固定为 "tcp"。
func (h *HostnameAddr) Network() string { return h.network }

// String 返回 "host:port" 形式的地址串 —— raft 配置里持久化的就是它, 所以它必须跨 pod 重建仍然有效。
func (h *HostnameAddr) String() string { return h.addr }

// TCPStreamLayer implements StreamLayer interface for plain TCP.
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewTCPTransport 在 TCP 流层上建一个 NetworkTransport, 日志写 logOutput、池上限 maxPool、
// I/O 超时 timeout, 12.8 的新开关一律取零值(关闭)。
// NewTCPTransport returns a NetworkTransport that is built on top of
// a TCP streaming transport layer.
func NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger 同上, 但日志走调用方传入的 hclog.Logger 而不是裸 io.Writer。
// NewTCPTransportWithLogger returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, with log output going to the supplied Logger
func NewTCPTransportWithLogger(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

// NewTCPTransportWithConfig 用完整配置结构建 transport; 建好后把刚创建的流层写回 config.Stream 再交给
// NewNetworkTransportWithConfig。
// NewTCPTransportWithConfig returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, using the given config struct.
func NewTCPTransportWithConfig(
	bindAddr string,
	advertise net.Addr,
	config *NetworkTransportConfig,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		config.Stream = stream
		return NewNetworkTransportWithConfig(config)
	})
}

// newTCPTransport 是三个 NewTCPTransport* 的共同底座: 先 bind, 再把监听器包成 TCPStreamLayer,
// 校验 advertise 地址可用后交给 transportCreator 建出 transport; 校验失败会关掉监听器并返回哨兵错误。
func newTCPTransport(bindAddr string,
	advertise net.Addr,
	transportCreator func(stream StreamLayer) *NetworkTransport) (*NetworkTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	// Verify that we have a usable advertise address. A concrete IP must be
	// advertisable (not 0.0.0.0); a HostnameAddr is accepted as-is so raft
	// stores the stable DNS name in its configuration.
	switch a := stream.Addr().(type) {
	case *net.TCPAddr:
		if a.IP == nil || a.IP.IsUnspecified() {
			_ = list.Close()
			return nil, errNotAdvertisable
		}
	case *HostnameAddr:
		if a.String() == "" {
			_ = list.Close()
			return nil, errNotAdvertisable
		}
	default:
		_ = list.Close()
		return nil, errNotTCP
	}

	// Create the network transport
	trans := transportCreator(stream)
	return trans, nil
}

// Dial 按 StreamLayer 接口拨一条出站 TCP 连接, 超时由 timeout 控制(不重试)。
// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept 直接转发给底层 *net.TCPListener, 本身不做任何限流或超时。
// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close 关闭底层监听器, 使 listen 循环里的 Accept 返回错误并走退避。
// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr 优先返回 advertise(对外宣告的地址), 未设置时才回落到实际绑定的监听地址。
// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
