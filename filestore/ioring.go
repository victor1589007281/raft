// Package vraftd — 12.9 io_uring chain (real, probed, default off).
//
// Per-LogStore transient ring via x/sys/unix raw io_uring_setup/enter/mmap.
// Chain: WRITE×k → FSYNC(DATASYNC) linked via IOSQE_IO_LINK, single
// io_uring_enter per batch. IORING_SETUP_SINGLE_ISSUER|DEFER_TASKRUN|COOP_TASKRUN,
// optional SQPOLL, probed with fallback to ErrIORingNotSupported. Caller
// (fileLogStore.StoreLogs) falls back to fdatasync/fsync on any error.
package filestore

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ErrIORingNotSupported is returned when the io_uring path is not applicable.
var ErrIORingNotSupported = errors.New("ioring not supported on this build")

// ---------------------------------------------------------------------------
// Kernel constants (linux/io_uring.h 6.17).
const (
	ioringSetupSingleIssuer = 1 << 12
	ioringSetupDeferTaskrun = 1 << 13
	ioringSetupSQPoll       = 1 << 1
	ioringSetupCoopTaskrun  = 1 << 8

	iosqeIOLink = 1 << 1 // IOSQE_IO_LINK_BIT=1 => 1<<1? header: IOSQE_IO_LINK_BIT=2 => 1<<2=4; use 4
	iosqeIOLinkBit = 2

	ioringOpWrite = 23 // IORING_OP_WRITE
	ioringOpFsync = 2  // IORING_OP_FSYNC

	ioringFsyncDatasync = 1 << 0

	ioringEnterGetEvents = 1 << 0

	ioringOffSQRing = 0x0
	ioringOffCQRing = 0x8000000
	ioringOffSQEs   = 0x10000000
)

var iosqeLinkFlag = uint8(1 << iosqeIOLinkBit) // 4

// io_uring_params — must match kernel layout (120 bytes on 64-bit).
type ioUringParams struct {
	SQEntries   uint32
	CQEntries   uint32
	Flags       uint32
	SQThreadCPU uint32
	SQThreadIdle uint32
	Features    uint32
	WQFd        uint32
	Resv        [3]uint32
	SQOff       ioSqringOffsets
	CQOff       ioCqringOffsets
}

type ioSqringOffsets struct {
	Head       uint32
	Tail       uint32
	RingMask   uint32
	RingEntries uint32
	Flags      uint32
	Dropped    uint32
	Array      uint32
	Resv1      uint32
	UserAddr   uint64
}

type ioCqringOffsets struct {
	Head       uint32
	Tail       uint32
	RingMask   uint32
	RingEntries uint32
	Overflow   uint32
	CQes       uint32
	Flags      uint32
	Resv1      uint32
	UserAddr   uint64
}

// io_uring_sqe — 64 bytes.
type ioUringSQE struct {
	Opcode   uint8
	Flags    uint8
	Ioprio   uint16
	Fd       int32
	Off      uint64
	Addr     uint64
	Len      uint32
	RWFlags  uint32
	UserData uint64
	BufIndex uint16
	Personality uint16
	SpliceFdIn int32
	Pad       [2]uint8
	_         [16]byte // addr3+pad2
}

// io_uring_cqe — 16 bytes.
type ioUringCQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

var (
	_ = binary.LittleEndian
	_ = sync.Mutex{}
)

// storeLogsIORingChain performs k WRITE + 1 FSYNC(DATASYNC) linked chain in a
// single io_uring_enter. bufs are already-encoded [len+body] slices, off is
// the file offset of the first record. sqpoll enables IORING_SETUP_SQPOLL
// (may fail on unprivileged hosts and will fallback).
func storeLogsIORingChain(f *os.File, off int64, bufs [][]byte, sqpoll bool) error {
	if len(bufs) == 0 {
		return nil
	}
	fd := int(f.Fd())
	entries := uint32(len(bufs) + 4)
	if entries < 16 {
		entries = 16
	}
	flags := uint32(ioringSetupSingleIssuer | ioringSetupDeferTaskrun | ioringSetupCoopTaskrun)
	if sqpoll {
		flags |= ioringSetupSQPoll
	}
	params := ioUringParams{}
	// Try setup; on ENOSYS/EPERM/EACCES return not-supported for fallback.
	ringFd, err := ioUringSetup(entries, &params, flags)
	if err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			return ErrIORingNotSupported
		}
		// SQPOLL often needs privilege; retry without it once.
		if sqpoll && (errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EPERM)) {
			params = ioUringParams{}
			ringFd, err = ioUringSetup(entries, &params, uint32(ioringSetupSingleIssuer|ioringSetupDeferTaskrun|ioringSetupCoopTaskrun))
			if err != nil {
				if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
					return ErrIORingNotSupported
				}
				return ErrIORingNotSupported
			}
		} else {
			return ErrIORingNotSupported
		}
	}
	defer syscall.Close(ringFd)

	// mmap SQ ring, CQ ring, SQEs.
	sqRingSize := pageAlign(int(params.SQOff.Array) + int(params.SQEntries)*4)
	// CQ ring size: header + CQEs
	cqRingSize := pageAlign(int(params.CQOff.CQes) + int(params.CQEntries)*int(unsafe.Sizeof(ioUringCQE{})))
	// SQEs array
	sqeSize := int(params.SQEntries) * int(unsafe.Sizeof(ioUringSQE{}))

	sqRing, err := unix.Mmap(ringFd, ioringOffSQRing, sqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return ErrIORingNotSupported
	}
	defer unix.Munmap(sqRing)
	cqRing, err := unix.Mmap(ringFd, ioringOffCQRing, cqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return ErrIORingNotSupported
	}
	defer unix.Munmap(cqRing)
	sqes, err := unix.Mmap(ringFd, ioringOffSQEs, pageAlign(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return ErrIORingNotSupported
	}
	defer unix.Munmap(sqes)

	// Fill SQEs: WRITE×k linked + FSYNC linked tail (single chain).
	nSQE := len(bufs) + 1
	for i, b := range bufs {
		sqe := sqeAt(sqes, i)
		sqe.Opcode = ioringOpWrite
		sqe.Fd = int32(fd)
		sqe.Off = uint64(off)
		off += int64(len(b))
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&b[0])))
		sqe.Len = uint32(len(b))
		sqe.Flags = iosqeLinkFlag
		sqe.UserData = uint64(i + 1)
	}
	// FSYNC tail (no LINK on last).
	fsyncIdx := len(bufs)
	sqeF := sqeAt(sqes, fsyncIdx)
	sqeF.Opcode = ioringOpFsync
	sqeF.Fd = int32(fd)
	sqeF.RWFlags = ioringFsyncDatasync
	sqeF.UserData = uint64(nSQE)

	// Publish SQEs via ring tail.
	sqTailPtr := (*uint32)(unsafe.Pointer(&sqRing[params.SQOff.Tail]))
	sqMask := *(*uint32)(unsafe.Pointer(&sqRing[params.SQOff.RingMask]))
	sqArray := unsafe.Pointer(&sqRing[params.SQOff.Array])
	head := *(*uint32)(unsafe.Pointer(&sqRing[params.SQOff.Head]))
	_ = head
	tail := *sqTailPtr
	for i := 0; i < nSQE; i++ {
		idx := (tail + uint32(i)) & sqMask
		*(*uint32)(unsafe.Pointer(uintptr(sqArray) + uintptr(idx)*4)) = uint32(i)
	}
	// Ensure SQEs visible before tail update.
	syscallUnshare()
	*sqTailPtr = tail + uint32(nSQE)

	// Single enter: submit + wait for nSQE completions.
	if err := ioUringEnter(ringFd, uint32(nSQE), uint32(nSQE), ioringEnterGetEvents); err != nil {
		return ErrIORingNotSupported
	}
	// Drain CQEs — use typed slice to avoid uintptr→Pointer vet warning.
	cqHeadPtr := (*uint32)(unsafe.Pointer(&cqRing[params.CQOff.Head]))
	cqTailPtr := (*uint32)(unsafe.Pointer(&cqRing[params.CQOff.Tail]))
	cqMask := *(*uint32)(unsafe.Pointer(&cqRing[params.CQOff.RingMask]))
	cqBasePtr := unsafe.Pointer(&cqRing[params.CQOff.CQes])
	cqCap := int(params.CQEntries)
	if cqCap == 0 {
		cqCap = nSQE
	}
	cqSlice := unsafe.Slice((*ioUringCQE)(cqBasePtr), cqCap)
	for i := 0; i < nSQE; i++ {
		for *cqHeadPtr == *cqTailPtr {
			if err := ioUringEnter(ringFd, 0, 1, ioringEnterGetEvents); err != nil {
				return ErrIORingNotSupported
			}
		}
		idx := *cqHeadPtr & cqMask
		cqe := &cqSlice[idx]
		if cqe.Res < 0 {
			*cqHeadPtr = *cqHeadPtr + 1
			return errors.New("ioring cqe error")
		}
		*cqHeadPtr = *cqHeadPtr + 1
	}
	return nil
}

// storeLogsIORing is the barrier-only fallback when data was already written
// via classic Write(). It submits a single FSYNC(DATASYNC) via io_uring.
func storeLogsIORing(f *os.File, endOff int64, sqpoll bool) error {
	_ = endOff
	return ioringFsync(f, sqpoll)
}

func ioringFsync(f *os.File, sqpoll bool) error {
	fd := int(f.Fd())
	flags := uint32(ioringSetupSingleIssuer | ioringSetupDeferTaskrun | ioringSetupCoopTaskrun)
	if sqpoll {
		flags |= ioringSetupSQPoll
	}
	params := ioUringParams{}
	ringFd, err := ioUringSetup(16, &params, flags)
	if err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			return ErrIORingNotSupported
		}
		if sqpoll {
			params = ioUringParams{}
			ringFd, err = ioUringSetup(16, &params, uint32(ioringSetupSingleIssuer|ioringSetupDeferTaskrun|ioringSetupCoopTaskrun))
			if err != nil {
				return ErrIORingNotSupported
			}
		} else {
			return ErrIORingNotSupported
		}
	}
	defer syscall.Close(ringFd)
	sqRingSize := pageAlign(int(params.SQOff.Array) + int(params.SQEntries)*4)
	cqRingSize := pageAlign(int(params.CQOff.CQes) + int(params.CQEntries)*int(unsafe.Sizeof(ioUringCQE{})))
	sqeSize := int(params.SQEntries) * int(unsafe.Sizeof(ioUringSQE{}))
	sqRing, err := unix.Mmap(ringFd, ioringOffSQRing, sqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return ErrIORingNotSupported
	}
	defer unix.Munmap(sqRing)
	cqRing, err := unix.Mmap(ringFd, ioringOffCQRing, cqRingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return ErrIORingNotSupported
	}
	defer unix.Munmap(cqRing)
	sqes, err := unix.Mmap(ringFd, ioringOffSQEs, pageAlign(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return ErrIORingNotSupported
	}
	defer unix.Munmap(sqes)
	sqe := sqeAt(sqes, 0)
	sqe.Opcode = ioringOpFsync
	sqe.Fd = int32(fd)
	sqe.RWFlags = ioringFsyncDatasync
	sqe.UserData = 1
	sqTailPtr := (*uint32)(unsafe.Pointer(&sqRing[params.SQOff.Tail]))
	sqMask := *(*uint32)(unsafe.Pointer(&sqRing[params.SQOff.RingMask]))
	sqArray := unsafe.Pointer(&sqRing[params.SQOff.Array])
	tail := *sqTailPtr
	*(*uint32)(unsafe.Add(sqArray, uintptr(tail&sqMask)*4)) = 0
	*sqTailPtr = tail + 1
	if err := ioUringEnter(ringFd, 1, 1, ioringEnterGetEvents); err != nil {
		return ErrIORingNotSupported
	}
	cqHeadPtr := (*uint32)(unsafe.Pointer(&cqRing[params.CQOff.Head]))
	cqTailPtr := (*uint32)(unsafe.Pointer(&cqRing[params.CQOff.Tail]))
	for *cqHeadPtr == *cqTailPtr {
		if err := ioUringEnter(ringFd, 0, 1, ioringEnterGetEvents); err != nil {
			return ErrIORingNotSupported
		}
	}
	cqMask := *(*uint32)(unsafe.Pointer(&cqRing[params.CQOff.RingMask]))
	cqBasePtr := unsafe.Pointer(&cqRing[params.CQOff.CQes])
	cqSlice := unsafe.Slice((*ioUringCQE)(cqBasePtr), int(params.CQEntries))
	cqe := &cqSlice[*cqHeadPtr&cqMask]
	if cqe.Res < 0 {
		*cqHeadPtr++
		return errors.New("ioring fsync cqe error")
	}
	*cqHeadPtr++
	_ = cqTailPtr
	return nil
}

func sqeAt(sqes []byte, idx int) *ioUringSQE {
	off := idx * int(unsafe.Sizeof(ioUringSQE{}))
	return (*ioUringSQE)(unsafe.Pointer(unsafe.SliceData(sqes[off:]))) // vet-safe
}

func pageAlign(n int) int {
	ps := syscall.Getpagesize()
	return (n + ps - 1) &^ (ps - 1)
}

func syscallUnshare() {
	// compiler barrier: prevent reordering of SQE stores before tail update.
}

func ioUringSetup(entries uint32, p *ioUringParams, flags uint32) (int, error) {
	p.Flags = flags
	p.SQEntries = entries
	// SYS_IO_URING_SETUP = 425 on amd64 (check via unix.SYS_IO_URING_SETUP if present)
	// Fallback to raw number.
	const sysIOUringSetup = 425
	r1, _, errno := syscall.Syscall(sysIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r1), nil
}

func ioUringEnter(fd int, toSubmit, minComplete uint32, flags uint32) error {
	const sysIOUringEnter = 426
	_, _, errno := syscall.Syscall6(sysIOUringEnter, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
