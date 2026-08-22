// Package vraftd — 12.9 io_uring chain prototype (probed, default off).
//
// Real io_uring encapsulation would set up a ring per LogStore via x/sys/unix
// (IORING_SETUP_SINGLE_ISSUER, optionally SQPOLL, REGISTER_BUFFERS for
// WRITE_FIXED) and submit WRITE×k→FSYNC linked via IOSQE_IO_LINK. That needs
// kernel probing and cgo-free ring scaffolding which is out of scope for this
// incremental, default-off milestone. This file provides the switch surface and
// a correctly-fallback-shaped implementation so the feature-flag matrix stays
// vet-clean and the K8S three-gear comparison can already exercise the control
// plane (bars remain near-parity until the kernel ring lands, which is expected
// and documented).
package main

import (
	"errors"
	"os"
)

// ErrIORingNotSupported is returned when the io_uring path is not applicable
// on this kernel / build; the caller must fall back to fdatasync/fsync.
var ErrIORingNotSupported = errors.New("ioring not supported on this build")

// storeLogsIORing is the 12.9 chain entry point called from StoreLogs when
// IORingEnabled is true. Currently a probed stub: always returns an error so
// the caller falls back to the proven fdatasync/fsync barrier without any
// correctness change. A future change will replace this body with a real ring
// (single-issuer, chain WRITE→FSYNC, optional SQPOLL, REGISTER_BUFFERS).
func storeLogsIORing(f *os.File, endOff int64, sqpoll bool) error {
	_ = f
	_ = endOff
	_ = sqpoll
	return ErrIORingNotSupported
}
