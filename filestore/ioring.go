// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

// 12.9/12.14 io_uring chains (real, probed, default off).
//
// Per-call transient ring via x/sys/unix raw io_uring_setup/enter/mmap.
// Chains (linked via IOSQE_IO_LINK, single io_uring_enter per batch):
//   single fd:  WRITE×k → FSYNC(DATASYNC)              (legacy/unified)
//   dual fd:    WRITE(redo) → WRITE(meta) → FSYNC → FSYNC  (ref mode,
//               "one transaction, two writes" — 12.14.2)
// IORING_SETUP_SINGLE_ISSUER|DEFER_TASKRUN|COOP_TASKRUN, optional SQPOLL,
// probed with fallback to ErrIORingNotSupported; callers fall back to
// classic write+fdatasync on any error.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ErrIORingNotSupported is returned when the io_uring path is not applicable.
var ErrIORingNotSupported = errors.New("ioring not supported on this build")

// ---------------------------------------------------------------------------
// Kernel constants (linux/io_uring.h).
const (
	ioringSetupSingleIssuer = 1 << 12
	ioringSetupDeferTaskrun = 1 << 13
	ioringSetupSQPoll       = 1 << 1
	ioringSetupCoopTaskrun  = 1 << 8

	iosqeIOLinkBit = 2 // IOSQE_IO_LINK_BIT

	ioringOpWrite = 23 // IORING_OP_WRITE
	ioringOpFsync = 3  // IORING_OP_FSYNC (NOT 2 — that's WRITEV; a wrong opcode
	// here was masked for months by the mis-sized SQE struct below)

	ioringFsyncDatasync = 1 << 0

	ioringEnterGetEvents = 1 << 0

	ioringOffSQRing = 0x0
	ioringOffCQRing = 0x8000000
	ioringOffSQEs   = 0x10000000
)

var iosqeLinkFlag = uint8(1 << iosqeIOLinkBit) // 4

// io_uring_params — must match kernel layout (120 bytes on 64-bit).
type ioUringParams struct {
	SQEntries    uint32
	CQEntries    uint32
	Flags        uint32
	SQThreadCPU  uint32
	SQThreadIdle uint32
	Features     uint32
	WQFd         uint32
	Resv         [3]uint32
	SQOff        ioSqringOffsets
	CQOff        ioCqringOffsets
}

type ioSqringOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	UserAddr    uint64
}

type ioCqringOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	CQes        uint32
	Flags       uint32
	Resv1       uint32
	UserAddr    uint64
}

// io_uring_sqe — exactly 64 bytes (kernel ABI). A wrong size here silently
// shifts every SQE after the first (we shipped that bug once: Pad[2] pushed
// the reserved area off, sizeof became 72, the kernel read garbage SQEs and
// the FSYNC degraded to a NOP — caught by TestIORingChainLandsOnDisk).
// Layout per linux/io_uring.h: buf_index 40-41, personality 42-43,
// splice_fd_in 44-47, addr3 48-55, pad2 56-63.
type ioUringSQE struct {
	Opcode      uint8
	Flags       uint8
	Ioprio      uint16
	Fd          int32
	Off         uint64
	Addr        uint64
	Len         uint32
	RWFlags     uint32
	UserData    uint64
	BufIndex    uint16
	Personality uint16
	SpliceFdIn  int32
	_           [16]byte // addr3 + pad2 (offsets 48-63)
}

// io_uring_cqe — 16 bytes.
type ioUringCQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

var _ = binary.LittleEndian

// Compile-time ABI guard: the kernel reads SQEs at 64-byte strides.
var _ [64]struct{} = [unsafe.Sizeof(ioUringSQE{})]struct{}{}

// ---------------------------------------------------------------------------
// uring: one transient ring for one batch chain.
type uring struct {
	ringFd int
	params ioUringParams
	sqRing []byte
	cqRing []byte
	sqes   []byte
	n      uint32 // SQEs pushed
}

// newUring sets up a ring; ENOSYS/EPERM/EACCES (and SQPOLL privilege
// failures) collapse to ErrIORingNotSupported so callers fall back.
func newUring(entries uint32, sqpoll bool) (*uring, error) {
	if entries < 16 {
		entries = 16
	}
	baseFlags := uint32(ioringSetupSingleIssuer | ioringSetupDeferTaskrun | ioringSetupCoopTaskrun)
	params := ioUringParams{}
	flags := baseFlags
	if sqpoll {
		flags |= ioringSetupSQPoll
	}
	ringFd, err := ioUringSetup(entries, &params, flags)
	if err != nil && sqpoll {
		params = ioUringParams{}
		ringFd, err = ioUringSetup(entries, &params, baseFlags)
	}
	if err != nil {
		return nil, ErrIORingNotSupported
	}
	u := &uring{ringFd: ringFd, params: params}

	sqRingSize := pageAlign(int(params.SQOff.Array) + int(params.SQEntries)*4)
	cqRingSize := pageAlign(int(params.CQOff.CQes) + int(params.CQEntries)*int(unsafe.Sizeof(ioUringCQE{})))
	sqeSize := int(params.SQEntries) * int(unsafe.Sizeof(ioUringSQE{}))
	if u.sqRing, err = unix.Mmap(ringFd, ioringOffSQRing, sqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		u.close()
		return nil, ErrIORingNotSupported
	}
	if u.cqRing, err = unix.Mmap(ringFd, ioringOffCQRing, cqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		u.close()
		return nil, ErrIORingNotSupported
	}
	if u.sqes, err = unix.Mmap(ringFd, ioringOffSQEs, pageAlign(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		u.close()
		return nil, ErrIORingNotSupported
	}
	return u, nil
}

func (u *uring) close() {
	if u.sqRing != nil {
		_ = unix.Munmap(u.sqRing)
	}
	if u.cqRing != nil {
		_ = unix.Munmap(u.cqRing)
	}
	if u.sqes != nil {
		_ = unix.Munmap(u.sqes)
	}
	if u.ringFd > 0 {
		_ = syscall.Close(u.ringFd)
	}
}

// pushWrite appends a WRITE SQE (linked if link).
func (u *uring) pushWrite(fd int, off int64, buf []byte, link bool) {
	sqe := u.sqeAt(int(u.n))
	sqe.Opcode = ioringOpWrite
	sqe.Fd = int32(fd)
	sqe.Off = uint64(off)
	sqe.Addr = uint64(uintptr(unsafe.Pointer(unsafe.SliceData(buf))))
	sqe.Len = uint32(len(buf))
	if link {
		sqe.Flags = iosqeLinkFlag
	}
	u.n++
	sqe.UserData = uint64(u.n) // 1-based: which SQE, for CQE forensics
}

// pushFsync appends an FSYNC(DATASYNC) SQE (linked if link).
func (u *uring) pushFsync(fd int, link bool) {
	sqe := u.sqeAt(int(u.n))
	sqe.Opcode = ioringOpFsync
	sqe.Fd = int32(fd)
	sqe.RWFlags = ioringFsyncDatasync
	if link {
		sqe.Flags = iosqeLinkFlag
	}
	u.n++
	sqe.UserData = uint64(u.n)
}

// submitAndWait publishes all pushed SQEs and drains their completions in one
// io_uring_enter; any CQE error fails the whole chain (callers roll back).
func (u *uring) submitAndWait() error {
	if u.n == 0 {
		return nil
	}
	sqTailPtr := (*uint32)(unsafe.Pointer(&u.sqRing[u.params.SQOff.Tail]))
	sqMask := *(*uint32)(unsafe.Pointer(&u.sqRing[u.params.SQOff.RingMask]))
	sqArray := unsafe.Pointer(&u.sqRing[u.params.SQOff.Array])
	tail := *sqTailPtr
	for i := uint32(0); i < u.n; i++ {
		idx := (tail + i) & sqMask
		*(*uint32)(unsafe.Pointer(uintptr(sqArray) + uintptr(idx)*4)) = i
	}
	*sqTailPtr = tail + u.n

	if err := ioUringEnter(u.ringFd, u.n, u.n, ioringEnterGetEvents); err != nil {
		return ErrIORingNotSupported
	}
	cqHeadPtr := (*uint32)(unsafe.Pointer(&u.cqRing[u.params.CQOff.Head]))
	cqTailPtr := (*uint32)(unsafe.Pointer(&u.cqRing[u.params.CQOff.Tail]))
	cqMask := *(*uint32)(unsafe.Pointer(&u.cqRing[u.params.CQOff.RingMask]))
	cqBasePtr := unsafe.Pointer(&u.cqRing[u.params.CQOff.CQes])
	cqCap := int(u.params.CQEntries)
	if cqCap == 0 {
		cqCap = int(u.n)
	}
	cqSlice := unsafe.Slice((*ioUringCQE)(cqBasePtr), cqCap)
	for i := uint32(0); i < u.n; i++ {
		for *cqHeadPtr == *cqTailPtr {
			if err := ioUringEnter(u.ringFd, 0, 1, ioringEnterGetEvents); err != nil {
				return ErrIORingNotSupported
			}
		}
		cqe := &cqSlice[*cqHeadPtr&cqMask]
		if cqe.Res < 0 {
			*cqHeadPtr = *cqHeadPtr + 1
			return fmt.Errorf("ioring cqe[%d/%d] res=%d (%v)", i, u.n, cqe.Res, syscall.Errno(-cqe.Res))
		}
		*cqHeadPtr = *cqHeadPtr + 1
	}
	return nil
}

func (u *uring) sqeAt(idx int) *ioUringSQE {
	off := idx * int(unsafe.Sizeof(ioUringSQE{}))
	return (*ioUringSQE)(unsafe.Pointer(unsafe.SliceData(u.sqes[off:])))
}

// ---------------------------------------------------------------------------
// Entry points.

// storeLogsIORingChain: WRITE×k → FSYNC in one enter (single fd).
func storeLogsIORingChain(f *os.File, off int64, bufs [][]byte, sqpoll bool) error {
	if len(bufs) == 0 {
		return nil
	}
	u, err := newUring(uint32(len(bufs)+4), sqpoll)
	if err != nil {
		return err
	}
	defer u.close()
	cur := off
	for _, b := range bufs {
		u.pushWrite(int(f.Fd()), cur, b, true)
		cur += int64(len(b))
	}
	u.pushFsync(int(f.Fd()), false)
	return u.submitAndWait()
}

// storeLogsIORingDual: ref mode's "one transaction, two writes" (12.14.2) —
// one io_uring_enter covering both files.
//
// SQE order is per-file serialized: WRITE(redo)→FSYNC(redo)→WRITE(meta)→
// FSYNC(meta). NB: on Linux 6.17 (and likely others) a linked pair of WRITEs
// to two DIFFERENT files gets the second write ECANCELED (-125) — cross-fd
// links are fine as long as an FSYNC sits between the two writes, which the
// durability contract wants anyway (redo durable before the meta pointing at
// it). redoBuf may be empty (batch of inline entries only): then the redo
// WRITE and its FSYNC are skipped.
func storeLogsIORingDual(redoF *os.File, redoOff int64, redoBuf []byte, metaF *os.File, metaOff int64, metaBuf []byte, sqpoll bool) error {
	n := uint32(4)
	if len(redoBuf) == 0 {
		n = 2
	}
	u, err := newUring(n+2, sqpoll)
	if err != nil {
		return err
	}
	defer u.close()
	if len(redoBuf) > 0 {
		u.pushWrite(int(redoF.Fd()), redoOff, redoBuf, true)
		u.pushFsync(int(redoF.Fd()), true)
	}
	u.pushWrite(int(metaF.Fd()), metaOff, metaBuf, true)
	u.pushFsync(int(metaF.Fd()), false)
	return u.submitAndWait()
}

// ioringFsync is the barrier-only fallback when data was already written via
// classic Write().
func ioringFsync(f *os.File, sqpoll bool) error {
	u, err := newUring(16, sqpoll)
	if err != nil {
		return err
	}
	defer u.close()
	u.pushFsync(int(f.Fd()), false)
	return u.submitAndWait()
}

func pageAlign(n int) int {
	ps := syscall.Getpagesize()
	return (n + ps - 1) &^ (ps - 1)
}

func ioUringSetup(entries uint32, p *ioUringParams, flags uint32) (int, error) {
	p.Flags = flags
	const sysIOUringSetup = 425 // SYS_IO_URING_SETUP on amd64
	r1, _, errno := syscall.Syscall(sysIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r1), nil
}

func ioUringEnter(fd int, toSubmit, minComplete uint32, flags uint32) error {
	const sysIOUringEnter = 426 // SYS_IO_URING_ENTER on amd64
	_, _, errno := syscall.Syscall6(sysIOUringEnter, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
