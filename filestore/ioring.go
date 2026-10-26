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

	// 三段 mmap 的固定偏移(linux/io_uring.h 的 IORING_OFF_* 约定), 不是任意值。
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

// newUring 建一个一次性 ring(entries 不足 16 时提到 16), 带 SQPOLL 建不起来就
// 退回不带 SQPOLL 再试一次; 仍失败则统一返回 ErrIORingNotSupported 让调用方回退。
//
// newUring sets up a ring; ENOSYS/EPERM/EACCES (and SQPOLL privilege
// failures) collapse to ErrIORingNotSupported so callers fall back.
func newUring(entries uint32, sqpoll bool) (*uring, error) {
	// 16 是 SQ/CQ 的实用下限: 链最长 4 个 SQE, 16 足够且不给内核多要页。
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
	// SQPOLL 需要特权(CAP_SYS_ADMIN 或 RLIMIT 放宽); 失败时清零 params 重试
	// 一次不带 SQPOLL 的版本 —— 功能不受影响, 只是少了内核对 SQ 的轮询。
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

// close 解除 SQ/CQ/SQE 三块 mmap 并关闭 ring fd。ring 是「一次批次一个」的
// 一次性对象, 无并发使用者, 故不做任何加锁。
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

// pushWrite 往 SQ 追加一个 WRITE SQE(link 为真则与后续 SQE 链接成链)。
// 注意 buf 的地址被直接写进 SQE: 提交前调用方必须保证它不被移动/回收。
//
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
	// UserData 是 1-based 序号: 完成事件里带回它, 便于定位是链上第几个 SQE 出错。
	sqe.UserData = uint64(u.n) // 1-based: which SQE, for CQE forensics
}

// pushFsync 往 SQ 追加一个 FSYNC SQE(link 为真则链接); RWFlags 置 DATASYNC
// 表示只刷数据不刷元数据(fdatasync 语义, 比 fsync 少一次元数据日志提交)。
//
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

// submitAndWait 发布已入队的全部 SQE 并回收它们的完成事件; 任一 CQE 报错即整条
// 链失败(调用方回退到经典路径)。n 为 0 时是空操作。
//
// submitAndWait publishes all pushed SQEs and drains their completions in one
// io_uring_enter; any CQE error fails the whole chain (callers roll back).
//
// 算法解读：
//
//	原理：分三步。① 把 SQ 尾指针后面 n 个槽填上 SQE 序号(经 SQ array 间接表,
//	按 RingMask 回绕), 再一次性推进尾指针 —— 这一步是「发布」, 内核只认尾指针;
//	② 一次 io_uring_enter(toSubmit=n, minComplete=n) 提交并等待 n 个完成;
//	③ 从 CQ 逐个摘完成事件(按 Head/Tail 与 RingMask 回绕), CQ 空时用
//	minComplete=1 的 enter 补等一个。
//	关键不变量：① SQ 的写入必须发生在推进尾指针之前 —— 顺序反了内核会读到半写的
//	SQE; ② 发布出去的 SQE 引用的缓冲区必须在 enter 返回前保持有效且不被改写
//	(调用方靠 defer close 与栈上缓冲保证); ③ 链接语义(IOSQE_IO_LINK)下前一个
//	失败会让后续链接项以 ECANCELED 结束, 故任一 CQE 失败即整链失败是正确判据,
//	不能只看第一个; ④ 完成事件必须全部摘干净再 close, 否则 munmap 时内核仍在写。
//	代价与边界：一次批次一次 ring 建立(SETUP + 3 次 mmap + 关闭), 固定开销存在;
//	故只对「至少 2 个 SQE 的链」有净收益, 单次写不该走这条路。CQ 容量不足时会
//	溢出(内核丢事件), 这里用 CQEntries 声明足够容量来避免。
//	设计权衡：用「一次性 ring」而不是长期驻留的共享 ring, 换掉了跨 goroutine 的
//	SQ 竞争与生命周期管理 —— 代价是每次批次的 syscall 数(1 次 setup + 1 次 enter
//	+ 3 次 mmap)不减, 收益在「把 k 次写 + 2 次屏障压成一次 enter」。若目标是极限
//	吞吐, 长期 ring + SQPOLL 更优, 但那需要重新设计调用方的并发模型。
func (u *uring) submitAndWait() error {
	if u.n == 0 {
		return nil
	}
	sqTailPtr := (*uint32)(unsafe.Pointer(&u.sqRing[u.params.SQOff.Tail]))
	sqMask := *(*uint32)(unsafe.Pointer(&u.sqRing[u.params.SQOff.RingMask]))
	sqArray := unsafe.Pointer(&u.sqRing[u.params.SQOff.Array])
	tail := *sqTailPtr
	// SQ array 是「槽位 → SQE 下标」的间接表: 这里第 i 个槽指向第 i 个 SQE。
	for i := uint32(0); i < u.n; i++ {
		idx := (tail + i) & sqMask // &Mask 而非 %Entries: Entries 是 2 的幂
		*(*uint32)(unsafe.Pointer(uintptr(sqArray) + uintptr(idx)*4)) = i
	}
	// 推进尾指针 = 一次性发布全部 SQE(内核在此之后才可能看到它们)。
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
		// 内核没回填 CQEntries 时退回按已提交数开窗, 避免 0 长度切片。
		cqCap = int(u.n)
	}
	cqSlice := unsafe.Slice((*ioUringCQE)(cqBasePtr), cqCap)
	for i := uint32(0); i < u.n; i++ {
		// CQ 里还没有新事件时补一次 enter(minComplete=1)等一个 —— 首次 enter 已经
		// 要求 minComplete=n, 这里通常不会空转, 是给溢出/分批完成的兜底。
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

// sqeAt 返回 SQE 数组里第 idx 个槽的指针 —— 按 unsafe.Sizeof(ioUringSQE{})=64
// 的步长定位; 步长错了会静默错位(见 ioUringSQE 的 ABI 注释)。
func (u *uring) sqeAt(idx int) *ioUringSQE {
	off := idx * int(unsafe.Sizeof(ioUringSQE{}))
	return (*ioUringSQE)(unsafe.Pointer(unsafe.SliceData(u.sqes[off:])))
}

// ---------------------------------------------------------------------------
// Entry points.

// storeLogsIORingChain 在单个 fd 上用一次 io_uring_enter 提交 WRITE×k → FSYNC 链。
//
// storeLogsIORingChain: WRITE×k → FSYNC in one enter (single fd).
//
// 算法解读：
//
//	原理：把 k 段写和一个屏障全部链接(除最后一个外每个 SQE 都带 IOSQE_IO_LINK),
//	然后只调一次 io_uring_enter —— 内核按链序依次执行: 前一个完成才启动后一个,
//	最后一个 FSYNC 完成时整批已持久。
//	关键不变量：① 链上每一项必须「链接」到下一项(只有末项不链), 否则内核可能
//	并行发起, FSYNC 就会跑在 WRITE 之前 —— 屏障语义全废; ② 缓冲区(bufs)在
//	enter 返回前必须有效; ③ 单 fd 内偏移是连续累加的, 调用方必须保证 off 之后
//	的区间在本次批次里没有被别人写过(由上层持锁保证)。
//	代价与边界：省下的是 (k+1) 次 syscall → 1 次 enter, 外加省掉每次写之间的
//	用户态/内核态往返; 但 ring 的建立与 mmap 是固定成本, 所以 k 很小时不划算。
//	任何一步失败(链上任一 CQE 非零)都会让整链失败, 调用方需回退经典路径重做。
//	设计权衡：为什么用链而不是并发提交 k 个独立 SQE? 因为写+屏障之间存在真依赖,
//	并发提交要靠额外同步才能保证顺序, 而链把顺序直接交给内核, 用户态无需等待中间
//	完成事件。为什么不用 O_DSYNC/O_SYNC 打开文件省掉显式 FSYNC? 那会让每次写都
//	自带屏障, 在批量场景下退化为逐条 fsync。
func storeLogsIORingChain(f *os.File, off int64, bufs [][]byte, sqpoll bool) error {
	if len(bufs) == 0 {
		return nil
	}
	// +4: 给链尾 FSYNC 及 SQ 回绕留余量, 避免环满。
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
	// 末项不链接: 链在此结束。
	u.pushFsync(int(f.Fd()), false)
	return u.submitAndWait()
}

// storeLogsIORingDual 是 ref 模式的「一事务两文件」: 一次 io_uring_enter 覆盖
// redo.log 与 raft.log 两个 fd 的写与屏障。
//
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
//
// 算法解读：
//
//	原理：把两文件的四步操作(WRITE(redo) → FSYNC(redo) → WRITE(meta) →
//	FSYNC(meta))串成一条链, 一次 enter 提交。FSYNC(redo) 夹在两次 WRITE 之间既是
//	内核要求也是持久性契约要求的 —— 前者: 跨 fd 的两个 WRITE 直接相连会在
//	Linux 6.17 上让第二个写拿到 ECANCELED(-125), 中间隔一个 FSYNC 就正常; 后者:
//	redo 字节必须先持久, 之后才允许「指向它的 meta」落盘, 否则崩溃后会出现悬空指针。
//	关键不变量：① 链序不可调换 —— 任意重排都会破坏「redo 先于 meta 持久」;
//	② redoBuf 为空时(纯内联条目批次)必须同时跳过 redo 的 WRITE 与 FSYNC, 且
//	预先算好的容量 n 也要跟着变, 否则 newUring 会多要/少要 SQ 槽;
//	③ 两个 fd 都必须与调用方抓拍的 fd 一致(压实换刀会换掉 redoF)。
//	代价与边界：比单 fd 链多一层「跨 fd 链接」的内核约束, 该约束未写进 ABI 文档,
//	是实测结论(注释里已记录 Linux 6.17 的行为); 内核版本差异会改变是否命中 ECANCELED,
//	但无论如何「中间隔 FSYNC」都是安全形式。
//	设计权衡：为什么不拆成两次 enter(先 redo 后 meta)? 那样会多一次 syscall 与
//	一次用户态等待, 且两次 enter 之间的窗口里 meta 已经可能被别的写者插入;
//	合成一链后「一个事务」的语义由内核保证, 用户态只需一次等待。
func storeLogsIORingDual(redoF *os.File, redoOff int64, redoBuf []byte, metaF *os.File, metaOff int64, metaBuf []byte, sqpoll bool) error {
	// 容量: redo 空批时链只有 WRITE(meta)+FSYNC(meta) 两项。
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
		// 这个 FSYNC 不能省: 跨 fd 的两个 WRITE 直接相连会被内核判 ECANCELED。
		u.pushFsync(int(redoF.Fd()), true)
	}
	u.pushWrite(int(metaF.Fd()), metaOff, metaBuf, true)
	u.pushFsync(int(metaF.Fd()), false)
	return u.submitAndWait()
}

// ioringFsync 是「只补屏障」的路径: 数据已由经典 Write 落盘, 这里只发一个 FSYNC。
// 也用于把一次 fsync 单独走 io_uring(部分内核上比 fdatasync 系统调用更省)。
//
// ioringFsync is the barrier-only fallback when data was already written via
// classic Write().
func ioringFsync(f *os.File, sqpoll bool) error {
	// 16 是 SQ 容量的实用下限, 单 SQE 链用不到更多。
	u, err := newUring(16, sqpoll)
	if err != nil {
		return err
	}
	defer u.close()
	u.pushFsync(int(f.Fd()), false)
	return u.submitAndWait()
}

// pageAlign 把 n 向上取整到系统页大小。io_uring 的 SQ 环、CQ 环与 SQE 数组三段
// mmap 的长度都必须整页 —— 内核按页映射, 给不足会 mmap 失败。
func pageAlign(n int) int {
	ps := syscall.Getpagesize()
	return (n + ps - 1) &^ (ps - 1)
}

// ioUringSetup 直接发 SYS_IO_URING_SETUP(425) 建 ring, 返回 ring fd。
// flags 由调用方按需组合(见本文件顶部常量), 结果回填进 p。
func ioUringSetup(entries uint32, p *ioUringParams, flags uint32) (int, error) {
	p.Flags = flags
	const sysIOUringSetup = 425 // SYS_IO_URING_SETUP on amd64
	r1, _, errno := syscall.Syscall(sysIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r1), nil
}

// ioUringEnter 直接发 SYS_IO_URING_ENTER(426): toSubmit 是本次要提交的 SQE 数,
// minComplete 是至少等到的完成数, flags 置 IORING_ENTER_GETEVENTS 时顺带回收 CQE。
func ioUringEnter(fd int, toSubmit, minComplete uint32, flags uint32) error {
	const sysIOUringEnter = 426 // SYS_IO_URING_ENTER on amd64
	_, _, errno := syscall.Syscall6(sysIOUringEnter, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
