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

// NewHostnameAddr returns a HostnameAddr for the given "host:port" string.
func NewHostnameAddr(addr string) *HostnameAddr {
	return &HostnameAddr{network: "tcp", addr: addr}
}

func (h *HostnameAddr) Network() string { return h.network }

func (h *HostnameAddr) String() string { return h.addr }

// TCPStreamLayer implements StreamLayer interface for plain TCP.
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

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

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
