// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

// Package filestore is vraft's library-level file-backed LogStore +
// StableStore: an append-only, fsync/fdatasync-per-batch raft log with the
// 12.7/12.9/12.14 write-path capabilities behind explicit switches (all
// default off, probed with fallback):
//
//   - SingleWAL unified segment format (v2): raft log record IS the redo
//     record — self-describing file magic, per-record CRC32C torn-tail
//     detection, native [base|lsn] header fields, batch-tail 512B padding
//     (O_DIRECT prerequisite), sparse LSN→(index,off) index.
//   - RedoCodec injection: vraft never interprets redo semantics; the
//     business injects Split/Merge so base/lsn extraction (LogDB redo
//     envelope) lives outside the library. Nil codec keeps payload = Data.
//   - 12.9 kernel I/O knobs: fdatasync, fallocate preallocation,
//     sync_file_range writeback kick, fadvise, io_uring WRITE→FSYNC chain.
//
// On-disk auto-detection: the file format follows the FILE, not the flags —
// a v2 file is always read as v2 (magic), a legacy [u32][msgpack] file stays
// legacy even if SingleWAL is enabled, so flipping switches never corrupts
// existing data.
package filestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

var msgpackHandle codec.MsgpackHandle

// ---------------------------------------------------------------------------
// RedoCodec: business-injected redo envelope semantics (12.14 P2 hook).
//
// vraft treats Log.Data as opaque; the codec decides which part is the redo
// envelope (base/lsn) and which part is the payload stored after the raft
// msgpack header. LogDB injects a parser for its [base|lsn|len|data] /
// JSON {"base":..} envelopes; nil codec means base=lsn=0, payload=Data.
type RedoCodec interface {
	// Split extracts (base, lsn, payload) from l. lsn==0 means "no LSN",
	// the record is skipped by the sparse index.
	Split(l *raft.Log) (base, lsn uint64, payload []byte)
	// Merge rebuilds the original Data from envelope fields and payload.
	Merge(base, lsn uint64, payload []byte) []byte
}

// IndexLSNCodec is a demo/test codec: stamps lsn = raft index so the sparse
// index is fully exercisable without a real redo consumer (LookupByLSN(i)=i).
// The payload is Data itself; Merge is the identity.
type IndexLSNCodec struct{}

// Split 是 IndexLSNCodec 的演示实现: lsn 直接取 raft index, 使稀疏索引在
// 没有真实 redo 消费方时也能被完整验证 (LookupByLSN(i)==i); payload 就是
// Data 本身, 不做 envelope 拆分。
func (IndexLSNCodec) Split(l *raft.Log) (uint64, uint64, []byte) { return 0, l.Index, l.Data }

// Merge 是 Split 的逆: IndexLSNCodec 下 payload 即 Data, 原样返回(恒等)。
func (IndexLSNCodec) Merge(_, _ uint64, payload []byte) []byte { return payload }

// RedoSegment is one redo record's worth of payload, addressed by LSN.
type RedoSegment struct {
	LSN     uint64
	Payload []byte
}

// RedoSegmentCodec is the multi-segment extension of RedoCodec for ref mode:
// one raft entry may fold N redo segments (e.g. LogDB's WriteRedoBatcher
// cmdBatch). SplitSegments returns 0 segments for non-redo entries (they are
// stored inline in the meta file). SplitSegments/MergeSegments must round-trip
// Data byte-identically.
type RedoSegmentCodec interface {
	RedoCodec
	SplitSegments(data []byte) (base uint64, segs []RedoSegment)
	MergeSegments(base uint64, segs []RedoSegment) []byte
}

// RedoShapeCodec is the optional shape extension of RedoCodec used by v2
// unified records. SplitShape is Split plus isRedo: it tells the file layer
// whether payload is a split redo segment (Data must be rebuilt by Merge on
// reload) or opaque entry data (control commands such as checkpoint/lease/
// catalog: reload must return the payload verbatim, NOT re-envelope it).
//
// 为什么需要它: 旧契约里 Merge 恒被调用, 业务 codec 只能无条件重包信封 ——
// 对控制命令(0 段)就会凭空多出一层 redo 信封(unified 重载损坏控制命令)。
// 用记录级 flags 持久化形状后, 重载侧才能区分「redo 段」与「原样字节」。
//
// 兼容性: 未实现本接口的 codec 沿用旧行为 —— encodeV2 写 flags=0(无形状
// 信息), 重载对每条记录仍调用 Merge(与修复前逐字节一致)。
type RedoShapeCodec interface {
	RedoCodec
	// SplitShape 等价于 Split, 额外报告 payload 是否由 redo 信封拆出。
	// isRedo=false 时 payload 必须就是原始 Data(重载侧原样还原)。
	SplitShape(data []byte) (base, lsn uint64, payload []byte, isRedo bool)
}

// Mode selects the on-disk store layout for NEW files (existing files always
// keep their own format — auto-detected).
type Mode string

const (
	ModeLegacy  Mode = ""        // [u32][msgpack(Log)] — upstream-compatible
	ModeUnified Mode = "unified" // v2 single file: meta header + payload inline
	ModeRef     Mode = "ref"     // v2 dual file: raft.log = pointer meta, redo.log = data (12.14.3)
)

// ---------------------------------------------------------------------------
// Options: every knob defaults to off (zero value) — a zero Options is the
// upstream-equivalent legacy store.
type Options struct {
	// 12.7/12.14 single WAL.
	SingleWAL   bool       // write v2 unified format on NEW files (= ModeUnified)
	Mode        Mode       // explicit layout; overrides SingleWAL when set
	SparseIndex bool       // build LSN→(index,off) index (effective on v2/v1 files)
	Codec       RedoCodec  // nil = no redo envelope semantics
	Framer      RedoFramer // ref mode: redo record framing; nil = StdFramer

	// 12.9 kernel I/O knobs (probed with fallback).
	Fdatasync           bool  // Sync → Fdatasync (skip inode mtime flush)
	PreallocateSegments bool  // fallocate next segment eagerly
	SegmentSize         int64 // 0 = 64 MiB
	UseFadvise          bool  // DONTNEED truncated tail
	UseSyncFileRange    bool  // async writeback kick mid-batch
	IORing              bool  // io_uring single-enter chain (dual-fd in ref mode)
	IORingSQPoll        bool  // requires IORing
	DirectIO            bool  // O_DIRECT tier (v2 formats only; probed, falls back to buffered)

	// UnboundRedoLimit (12.14 P3): max bytes written via WriteRedoDirect but
	// not yet bound by a committed raft entry; 0 = unlimited. Over the limit
	// the direct write gets ErrUnboundFull (backpressure → caller degrades).
	UnboundRedoLimit int64

	Logger *log.Logger // optional; nil = silent
}

// Validate 校验开关之间的互锁(对齐 raft.Config.Validate): IORingSQPoll 必须
// 挂在 IORing 上, Mode 只接受 ""/unified/ref。刻意不把 SparseIndex 绑到
// SingleWAL —— 格式跟随文件, 关掉写开关后仍须能索引既有的 v2 文件(滚动重启兼容)。
// Validate enforces the switch interlocks (mirrors raft.Config.Validate).
// SparseIndex is deliberately NOT tied to SingleWAL here: the format follows
// the file, so a v2 file must be indexable even when the SingleWAL write
// switch is off (rolling-restart compatibility).
func (o Options) Validate() error {
	if o.IORingSQPoll && !o.IORing {
		return fmt.Errorf("filestore: IORingSQPoll requires IORing")
	}
	if o.Mode != "" && o.Mode != ModeUnified && o.Mode != ModeRef {
		return fmt.Errorf("filestore: unknown Mode %q", o.Mode)
	}
	return nil
}

// resolvedMode 把 (Mode, SingleWAL) 开关对折算成新建文件的实际布局: Mode
// 显式指定优先, 否则 SingleWAL ⇒ unified, 全关 ⇒ legacy。
// resolvedMode maps the switch pair to the effective new-file layout.
func (o Options) resolvedMode() Mode {
	if o.Mode != "" {
		return o.Mode
	}
	if o.SingleWAL {
		return ModeUnified
	}
	return ModeLegacy
}

// ---------------------------------------------------------------------------
// On-disk formats (little-endian).
//
// v2 file header (512 B, written once at creation):
//
//	[0:4]   magic "VWAL"
//	[4:6]   u16 formatMajor = 2
//	[6:8]   u16 fileFlags (bit0 singleWAL; bit1 batch-pad512)
//	[8:512] reserved zero
//
// v2 record (records start at offset 512):
//
//	[u32 totalLen]                     // = 24 + raftLen + dataLen + 4(crc)
//	[u16 ver=2][u16 flags]             // flags: bit0 recFlagRedo / bit1 recFlagShapeKnown
//	[u64 base][u64 lsn]
//	[u32 raftLen][msgpack(Log without Data)]
//	[payload]
//	[u32 crc32c over bytes [4 .. 4+totalLen-4)]
//
// flags 是记录级形状标志(与文件头 flags 无关): bit1=0 是修复前的旧记录, 重载
// 按旧语义恒走 Codec.Merge; bit1=1 时 bit0 决定 payload 是 redo 段(需 Merge
// 重建信封)还是原样字节(控制命令)。旧记录不需要迁移, 与带标志的新记录可混存。
//
// Batch: records concatenated, then zero padding to the next 512 B boundary.
// Padding is never part of any record; reload treats a zero u32 at a record
// boundary as "skip to next 512 B boundary once", and a second consecutive
// zero as end-of-data (fallocate'd regions read as zeros).
//
// v1 (prototype compat, read-only): [u32 totalLen][u16 ver=1][u16 flags]
//
//	[u64 base][u64 lsn][u32 raftLen][msgpack(Log without Data)][data] — no CRC.
//
// legacy: [u32 payloadLen][msgpack(Log)] repeated.
const (
	fileMagic     = "VWAL"
	formatV2      = 2
	fileHeaderLen = 512

	recHeaderV2 = 4 + 2 + 2 + 8 + 8 + 4 // totalLen+ver+flags+base+lsn+raftLen = 28
	recTrailV2  = 4                     // crc32

	fileFlagSingleWAL = 1 << 0
	fileFlagPad512    = 1 << 1

	// v2 记录级 flags(body[2:4], 与文件头 flags 无关), 由 encodeV2 写入、
	// decodeV2 解读:
	//   recFlagRedo       payload 是 redo 段: 重载时 Data = Codec.Merge(base,lsn,payload)
	//   recFlagShapeKnown recFlagRedo 有效(新写者)。旧记录 flags==0 ⇒ 形状未知,
	//                     重载保持旧行为(恒 Merge), 保证既有磁盘文件逐字节可读。
	// 旧文件里的控制命令没有形状标志, 无法无歧义恢复(redo 段的 base/lsn 都可以
	// 是 0, 不能用 0 值猜), 仍按旧语义重载; 新写记录一律带 shape 标志。
	// 降级注意: 旧二进制忽略记录 flags(恒 Merge), 读新文件里的控制命令仍会包
	// 一层 redo 信封 —— 回滚到修复前版本会重新暴露该 bug, 故不做格式版本升级,
	// 只让新读者按记录区分(新旧记录在文件里可共存)。
	recFlagRedo       = 1 << 0
	recFlagShapeKnown = 1 << 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type lsnPos struct {
	index uint64
	off   int64
}

// redoPos is an entry of the ref-mode redo presence map (in-memory only;
// rebuilt by scanning redo.log on reload).
type redoPos struct {
	off    int64
	length int
}

// errScanStop is the internal sentinel for torn/corrupt tails in scanRecords.
var errScanStop = errors.New("filestore: torn tail")

// scanStats accumulates what scanRecords observed beyond the good tail.
// #163 W4: hole healing needs to know how many bytes it skipped and whether
// the skip was a tolerated hole (healed) or the end of data.
type scanStats struct {
	holes      int   // tolerated 512 对齐零洞个数(见下方判别式)
	holesBytes int64 // bytes covered by tolerated skips
}

// scanRecords 从 start 起顺序走 [u32 len][body] 记录, 返回好数据终点(下一次
// 追加的位置); 只读扫描, 不落盘 —— 重载、空洞治愈与撕尾截断都以它为准。
// scanRecords walks [u32 len][body] records starting at start. When
// padBatches (v2 formats), a zero length at a record boundary is batch-tail
// padding: skip to the next 512B boundary (off advances even on break so
// endOff stays batch-aligned); zeros running to EOF = end of data.
//
// #163 W4 行级根修(孔洞容忍): WriteRedoDirect 二阶段写失败回滚只退记账、不退
// redoEnd 指针, 在 redo.log 中留下 512 对齐的零洞; 并发乱序发布可令洞先于
// 已发布记录落位。旧扫描的"两个连续零段 = 数据终点"启发式在真实 #163 指纹
// (批尾填充零段 + 紧邻孔洞零段 = 貌似两个连续零段, 但后面还有活记录)下
// 依旧提前 break → 引用洞后字节的一切已提交 meta 丢光(cascade truncate)
// → GetLog not-found → commit=0 死锁家族。现改为逐 512 块判定:
//   - 零块向后推进, 直到遇到非零块头(可能是有记录也可能是垃圾)或 EOF;
//   - 非零块头交给 yield/调用方解码: 合法记录 = 孔洞被治愈(stats.holes++),
//     解码失败(errScanStop) = 撕尾, 停在洞后第一个垃圾块头 —— 与旧
//     torn-tail 语义一致;
//   - 零块一路到 EOF = 批尾填充, 数据终点(批对齐不变量保留)。
//
// 返回好数据终点(下一追加位置)。
// 算法解读：
//
//	原理：逐 512B 块推进。读到非零长度头就按 [u32 len][body] 取一条记录交给
//	  yield 解码; 读到零长度头(且 padBatches)则视为批尾填充或零洞, 向后吞掉
//	  连续零块直到非零块头或 EOF。零区之后若跟的是可解码记录, 说明它是被治愈
//	  的洞; 若解码失败(errScanStop)则停在坏块头前, 与旧撕尾语义一致。
//	关键不变量：off 与返回的 endOff 恒 512 对齐 —— 零洞判别式
//	  (zeroStart%512==0 || pendingZero>=512)正是靠它区分"批尾部分槽填充"与
//	  "WriteRedoDirect 回滚留下的整段保留槽"; 好数据终点之前绝不允许丢弃
//	  已提交 meta 引用的字节, 否则触发 cascade truncate → GetLog not-found
//	  → commit=0 死锁家族。
//	代价与边界：O(文件字节), 一次线性扫描; 判别式只在"零区后紧跟记录"时生效,
//	  零区一路到 EOF 一律判为数据终点(批尾填充), 故被截断的洞会停在最后一个
//	  完整块边界。非 512 对齐的零头保持旧语义(直接判终点), 不把未知格式当洞治愈。
//	设计权衡：旧启发式"两个连续零段 = 终点"在 #163 指纹(批尾填充零段紧邻回滚
//	  洞零段)下提前 break, 会丢光洞后一切已提交 meta。改为逐块判定, 是用一次
//	  额外解码尝试换取"绝不因启发式误判而丢数据"。
func scanRecords(raw []byte, start int64, padBatches bool, yield func(off int64, body []byte) error) (int64, scanStats, error) {
	buf := bytes.NewReader(raw[start:])
	off := start
	var stats scanStats
	zeroStart := int64(-1)  // 最近一次跳过零区的起始偏移(-1 = 无)
	pendingZero := int64(0) // 最近一次跳过的零块字节数(可能是填充也可能是孔洞)
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(buf, lenBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return off, stats, err
		}
		n := int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if n <= 0 {
			// 洞/批尾填充只认 512 对齐形态(本格式一切记录与洞都是 512 对齐的);
			// 不对齐的零 = 未知格式, 保持旧语义(终点)。终点停在批边界(可能
			// 越过 EOF —— redoEnd 恒 512 对齐的不变量必须保)。
			if !padBatches {
				break
			}
			next := align512(off)
			pendingZero = next - off
			if zeroStart < 0 {
				zeroStart = off
			}
			off = next
			// 零块向后推进: 跳过连续零块, 直到非零块头/EOF。
			for off+4 <= int64(len(raw)) {
				if binary.LittleEndian.Uint32(raw[off:off+4]) != 0 {
					break // 洞后出现非零块头: 可能是记录也可能是垃圾, 交外层判定
				}
				pendingZero += 512
				off += 512
			}
			if off+4 > int64(len(raw)) {
				// 零块一路到 EOF: 批尾填充, 数据终点。若 raw 长度不是 512 的
				// 倍数(截断的洞), 停在最后一个完整块边界。
				break
			}
			if _, err := buf.Seek(off-start, io.SeekStart); err != nil {
				return off, stats, err
			}
			continue
		}
		if pendingZero > 0 {
			// 零区之后跟的是记录 = 填充或孔洞被确认。判别式: 批尾填充是
			// 【部分槽】(记录尾字节之后到槽边, < 512 且起始偏移非 512 对齐);
			// WriteRedoDirect 回滚洞是【整段保留槽】(redoEnd 原本就 512 对齐,
			// 整段零) —— 零区起始落在 512 边界, 或涵盖至少一个完整 512 块。
			if zeroStart%512 == 0 || pendingZero >= 512 {
				stats.holes++
				stats.holesBytes += pendingZero
			}
			pendingZero = 0
			zeroStart = -1
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(buf, body); err != nil {
			break // crash-truncated tail
		}
		if err := yield(off, body); err != nil {
			if err == errScanStop {
				// 撕尾/垃圾: 停在洞后第一个坏块头之前(off 已含洞), 截断语义
				// 与旧实现一致(下一追加从 good end 覆盖)。
				break
			}
			return off, stats, err
		}
		off += 4 + n
	}
	return off, stats, nil
}

// Store is an append-only file-backed raft.LogStore.
//
// 锁契约(12.14 读锁分离):
//   - mu(RWMutex) 护全部内存状态与文件偏移(logs/offsets/redoMap/refSegs/
//     refBases/lsnIndex/redoEnd/unboundBytes/fd 身份)。写者 Lock; 纯内存
//     查询 RLock。redo.log 记录 append-only 且落位后不可变(去重重写 = 追加
//     新版本 + 换 map 指针, 绝不原地覆盖), 故读者在 RLock 内抓拍 (fd,off,len)
//     后即可【锁外读盘】—— apply 路径的 mergeEntry 重建读不再与 StoreLogs
//     的 fsync / 直发写在大锁里互相挤(G 相对 G0 的 -20% 税的来源)。
//   - redoIoMu(RWMutex) 只钉 redoF 身份: 锁外读者在 ReadAt 期间持 RLock;
//     CompactRedo 换刀(rename 后关旧 fd)持 Lock。锁序 mu → redoIoMu
//     (读者先放 mu 再取 redoIoMu, 压实同时持两把, 无环)。
type Store struct {
	mu       sync.RWMutex
	redoIoMu sync.RWMutex
	path     string
	f        *os.File // raft.log (ref mode: meta records)
	opts     Options
	first    uint64 // index of logs[0] (0 when empty)
	logs     []*raft.Log
	offsets  []int64 // file offset of logs[i] (points at the record's len field)
	endOff   int64   // authoritative append position (512-aligned when v2)

	format   string // "v2-ref" | "v2-singlewal" | "v1-singlewal" | "legacy" — detected
	directIO bool   // effective after probe (v2 formats only)

	// ref mode only: redo.log is the data file; raft.log holds pointer meta.
	redoF    *os.File
	redoEnd  int64              // redo.log good-data end (append position)
	redoMap  map[uint64]redoPos // lsn → presence (data authority for reads)
	refSegs  [][]uint64         // parallel to logs: segment LSNs of entry i (nil = inline)
	refBases []uint64           // parallel to logs: base of entry i

	lsnIndex map[uint64]lsnPos // sparse index (only when SparseIndex)
	ioringOK uint64            // io_uring chains actually completed (probe evidence)

	unboundBytes int64 // P3: direct-written bytes not yet bound by raft meta
	// J5 泄漏修复(9.24): 每条直发预留的 per-LSN 台账。同 LSN 的块渐满重写
	// (InnoDB 512B 块随填充重发)与快照演化会产生多代预留, 旧的 bound-keep /
	// stale-keep / rewrite 分支必须释放被替代的那代, 否则水位虚高单调涨满
	// UnboundRedoLimit → 直发通道永久背压(K6 实测 60% 预留泄漏, 268MiB 钉死)。
	// 键=LSN, 值=该 LSN 当前存活预留(可有多代并存? 不: 重写即替代, 每 LSN
	// 至多一条存活预留 + 一条待发布预留 —— 均以 recLen 累计于此)。
	unboundPending map[uint64]int64

	// #163 W4 孔洞观测: reloadRef 越过的 512 对齐零洞计数与字节数(进程累计)。
	redoHealedHoles uint64
	redoHoleBytes   int64

	// J5 绑定速率观测(进程累计): directRedoSegs = WriteRedoDirect 成功发布
	// 的段数; boundRedoSegs/BoundBytes = raft meta 首次建 lsnIndex 引用的段数/
	// 字节数(绑定时刻)。Δbind/Δt 即「绑定 segs/s」—— J5 多 raft 组立项判据
	// 挂它的实测饱和点, 不挂 tps。
	directRedoSegs uint64
	boundRedoSegs  uint64
	boundRedoBytes int64
}

var _ raft.LogStore = (*Store)(nil)

// Open 打开(或创建) dir 下的日志存储并重载内容; 磁盘格式自动探测, opts 的
// SingleWAL/Mode 只影响本次调用新建的文件。ref 模式下 dir 内还有数据文件
// redo.log。reload 后按需做 O_DIRECT 探测(仅 v2 格式), 失败即回退缓冲。
// Open opens (or creates) the log store in dir and reloads its contents.
// The on-disk format is auto-detected; opts.SingleWAL/Mode only affect files
// created by this call. In ref mode dir also holds redo.log (the data file).
func Open(dir string, opts Options) (*Store, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{path: path, f: f, opts: opts}

	// ★一次性 fencing 特赦(2026-10-09, 4920271805 洞实案): 上代从 log buffer 抢跑
	// ship(比本地盘快), 崩溃把「仅 buffer」的尾字节绑定进存储; 本代从盘恢复
	// (不含该尾) ⇒ 同 LSN 两种内容, 幂等吸收器按 fencing 保留死代 ⇒ frontier
	// 永冻。env LOGDB_FILESTORE_FENCING_OVERRIDE_LSN=<lsn> 指定洞点: 该 LSN 的
	// 绑定异变段改走重写(死代让位真历史), 审计双内容 FNV 留证。仅指定 LSN 生效,
	// 用后须摘除 env(config hygiene)。
	if v := os.Getenv("LOGDB_FILESTORE_FENCING_OVERRIDE_LSN"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n != 0 {
			fencingOverrideLSN = n
			if opts.Logger != nil {
				opts.Logger.Printf("[WARN] filestore: fencing 特赦已武装: LSN=%d —— 该点的绑定异变将以新内容覆盖死代并审计双内容 FNV", n)
			}
		}
	}
	// 任意 LSN 特赦(临时手术模式): 崩溃代缓冲抢跑的死尾会在【每个】崩溃边界产叉,
	// 逐 LSN 特赦赶不上产速。单写者秩序已由 RPC 层 writer_epoch fencing 保证,
	// 此模式下任何 LSN 的绑定异变都判定为死代并让位于当前代, 全量 FNV 审计。
	// ★仅排雷期临时武装, 净空后必须摘除。
	if os.Getenv("LOGDB_FILESTORE_FENCING_KEEP_OLD") != "" {
		fencingKeepOld.Store(true)
		if opts.Logger != nil {
			opts.Logger.Printf("[WARN] filestore: fencing 旧行为(冻结保留旧字节)已武装 —— 排雷期外请勿使用")
		}
	}
	if os.Getenv("LOGDB_FILESTORE_FENCING_OVERRIDE_ALL") != "" {
		fencingOverrideAll = true
		if opts.Logger != nil {
			opts.Logger.Printf("[WARN] filestore: ★fencing 特赦【全 LSN】模式已武装(临时排雷手术) —— 任何绑定异变都将以当前代内容覆盖死代并审计 FNV, 净空后摘除 env!")
		}
	}

	metaInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	metaNew := metaInfo.Size() == 0

	// Resolve the layout: existing files keep their own format (detected in
	// reload); new files follow the switches.
	wantRef := opts.resolvedMode() == ModeRef
	if wantRef || !metaNew {
		// ref pairing checks happen once the format is known; open redo.log now
		// if it exists or ref is requested.
		redoPath := filepath.Join(dir, redoFileName)
		if _, err := os.Stat(redoPath); err == nil || wantRef {
			rf, err := os.OpenFile(redoPath, os.O_CREATE|os.O_RDWR, 0o644)
			if err != nil {
				_ = f.Close()
				return nil, err
			}
			ri, err := rf.Stat()
			if err != nil {
				_ = rf.Close()
				_ = f.Close()
				return nil, err
			}
			if metaNew && ri.Size() > 0 {
				_ = rf.Close()
				_ = f.Close()
				return nil, fmt.Errorf("filestore: %s exists (%d B) without its meta file — refusing to guess", redoPath, ri.Size())
			}
			s.redoF = rf
		}
	}

	if err := s.reload(); err != nil {
		_ = f.Close()
		if s.redoF != nil {
			_ = s.redoF.Close()
		}
		return nil, err
	}

	// O_DIRECT tier: only for v2 formats (batch-tail padding guarantees
	// offset/length alignment); probed per file, falls back to buffered.
	// 只对 v2 格式探测: 批尾 512B 填充保证了偏移/长度对齐, 这是 O_DIRECT 的前提;
	// 探测失败不报错 —— 静默退回 buffered(s.directIO 保持 false)。
	if opts.DirectIO && (s.format == "v2-singlewal" || s.format == "v2-ref") {
		if df, ok := reprobeDirect(path, f, opts.Logger); ok {
			s.f = df
			s.directIO = true
			_ = f.Close()
		}
		if s.format == "v2-ref" && s.redoF != nil {
			if df, ok := reprobeDirect(filepath.Join(dir, redoFileName), s.redoF, opts.Logger); ok {
				_ = s.redoF.Close()
				s.redoF = df
			} else if !s.directIO {
				// keep consistent: both files buffered
			} else if opts.Logger != nil {
				opts.Logger.Printf("[WARN] filestore: redo.log O_DIRECT probe failed, redo writes stay buffered")
			}
		}
	}
	if opts.Logger != nil {
		opts.Logger.Printf("[INFO] filestore: %s format=%s directIO=%v first=%d last=%d endOff=%d",
			path, s.format, s.directIO, s.first, s.lastIndexLocked(), s.endOff)
	}
	return s, nil
}

// reprobeDirect 用 O_DIRECT 重开 path 并做一次对齐 pwrite 探测; 任何一步失败
// 都静默回退缓冲模式(探测回退), 绝不让不支持 O_DIRECT 的盘把存储开死。
// reprobeDirect reopens path with O_DIRECT and probes an aligned pwrite.
// On any failure the store stays buffered (探测回退).
// 算法解读：
//
//	原理：Stat 取原文件长度 → 以 O_DIRECT 重开 → 在 1MiB 处写一个 512B 对齐
//	  缓冲 → 成功后 Truncate 回原长度。探测点选在 1MiB 而非偏移 0, 是为了
//	  绝不碰头部 512B 的 magic/header。
//	关键不变量：探测写必须 512B 对齐且长度对齐 —— 未对齐的 O_DIRECT 写会
//	  EINVAL, 会被误判成"盘不支持"而白白退回缓冲; Truncate 回 info.Size()
//	  必须成功, 否则文件被撑大, 尾部多出的零会被扫描器当批尾填充吞掉。
//	代价与边界：每个文件一次探测写 + 一次 ftruncate, 只在 Open 时付一次。
//	  注意探测点是固定的 1MiB: 文件已超过 1MiB 时, 这 512B 探测写会覆盖真实
//	  字节, 而 Truncate 回原长度并不能恢复它们(见汇报里的"发现的问题")。
//	设计权衡：O_DIRECT 在部分文件系统/内核组合上只是"打开成功但写失败",
//	  故必须真写一次才能判定; 用探测回退而不是硬失败, 让同一份配置能在
//	  tmpfs/NFS/overlayfs 上照常跑(只是退回 buffered)。
func reprobeDirect(path string, cur *os.File, logger *log.Logger) (*os.File, bool) {
	info, err := cur.Stat()
	if err != nil {
		return nil, false
	}
	df, err := os.OpenFile(path, os.O_RDWR|unix.O_DIRECT, 0o644)
	if err != nil {
		if logger != nil {
			logger.Printf("[WARN] filestore: O_DIRECT open %s: %v (buffered fallback)", path, err)
		}
		return nil, false
	}
	buf := alignedBytes(512)
	// 1MiB 是选定的探测点: 远超常规数据区(避免碰头部 magic), 写完立刻 Truncate
	// 回原长度抹掉。注意: 文件本身已 >1MiB 时这 512B 写会覆盖真实字节, 且
	// Truncate 回同长度无法恢复(见汇报的"发现的问题")。
	probeAt := int64(1 << 20) // beyond any real data; truncated back afterwards
	if _, err := df.WriteAt(buf, probeAt); err != nil {
		_ = df.Close()
		if logger != nil {
			logger.Printf("[WARN] filestore: O_DIRECT probe write %s: %v (buffered fallback)", path, err)
		}
		return nil, false
	}
	_ = df.Truncate(info.Size())
	return df, true
}

// Format 返回探测到的磁盘格式("v2-singlewal"/"v2-ref"/"v1-singlewal"/"legacy"),
// 它是文件自身的格式, 与当前开关无关。持 RLock。
// Format returns the detected on-disk format ("v2-singlewal" / "v1-singlewal" /
// "legacy") — the file's own format, independent of the current switches.
func (s *Store) Format() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.format
}

// IsMonotonic implements raft.MonotonicLogStore: filestore 是位置式追加存储
// (first + 连续索引算术), 无法表示快照安装留下的低位空洞 —— 因此快照安装时
// 走 removeOldLogs 全量删除(而非保留尾部的 compactLogs), 让 first 归零重铺,
// 也保证 StoreLogs 连续性护栏永不误伤合法的快照重接。
func (s *Store) IsMonotonic() bool { return true }

// Close 先 Sync 主文件与 redo.log, 再取 redoIoMu 写锁等锁外读者(mergeEntry/
// ReadRedo*)的 in-flight ReadAt 收工后关 fd; 任一 Sync 失败即提前返回, 不保证
// 后续句柄都被关闭。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		return err
	}
	if s.redoF != nil {
		if err := s.redoF.Sync(); err != nil {
			return err
		}
		// 等锁外读者(mergeEntry/ReadRedo*)的 in-flight ReadAt 收工再关 fd。
		s.redoIoMu.Lock()
		defer s.redoIoMu.Unlock()
		if err := s.redoF.Close(); err != nil {
			return err
		}
	}
	return s.f.Close()
}

// lastIndexLocked 返回最后一条日志的 index, 空日志返回 0。调用方须持有 s.mu。
func (s *Store) lastIndexLocked() uint64 {
	if len(s.logs) == 0 {
		return 0
	}
	return s.logs[len(s.logs)-1].Index
}

// ---------------------------------------------------------------------------
// Encoding.

// encodeLogMsgpack 用 msgpack 编码一条 raft.Log(调用方须已把 Data 摘掉), 供
// legacy/v1/v2 三条编码路径复用。
func encodeLogMsgpack(l *raft.Log) ([]byte, error) {
	var buf []byte
	if err := codec.NewEncoderBytes(&buf, &msgpackHandle).Encode(l); err != nil {
		return nil, err
	}
	return buf, nil
}

// encodeV2 构造一条 v2 记录: codec 把 Data 拆成 envelope(base/lsn, 存进原生
// 头部字段)与 payload(存在 raft msgpack 头之后), 同一份字节同时服务 raft 视图
// (GetLog)与 redo 视图(LookupByLSN/裸扫描), 数据不重复落盘。
// encodeV2 builds one v2 record. The codec splits Data into envelope
// (base/lsn, stored in native header fields) and payload (stored after the
// raft msgpack header), so the same bytes serve the raft view (GetLog) and
// the redo view (LookupByLSN / raw scan) without duplication.
// 算法解读：
//
//	原理：布局 = [u32 totalLen][u16 ver=2][u16 flags][u64 base][u64 lsn]
//	  [u32 raftLen][msgpack(Log 去掉 Data)][payload][u32 crc32c]。CRC 覆盖
//	  [4 .. len-4), 即除长度字段外的全部字节 —— 长度字段自身不参与校验,
//	  因为扫描器必须先读它才能知道记录边界。
//	关键不变量：totalLen = (recHeaderV2-4) + raftLen + payloadLen + 4, 与实际
//	  写入长度必须一致, 否则下一条记录的起点整体错位; base/lsn 是原生字段而非
//	  msgpack 内嵌, 使 redo 视图无需解码 msgpack 即可定位。
//	代价与边界：一次 msgpack 编码 + 一次全记录 CRC32C(软件表), payload 走 copy
//	  而非零拷贝; nil codec 时 base=lsn=0, payload=Data, 稀疏索引因 lsn==0 跳过。
//	设计权衡：把 envelope 提到原生字段而不是塞进 msgpack, 换来 redo 侧零解码
//	  读取与可算的记录长度(512 对齐的前提, O_DIRECT 需要); 代价是格式不自
//	  描述, 必须靠文件头 magic + ver 判定。
func (s *Store) encodeV2(l *raft.Log) (rec []byte, base, lsn uint64, err error) {
	payload := l.Data
	// isRedo 决定重载侧是否用 Codec.Merge 重建 Data。优先问 RedoShapeCodec
	// (控制命令 ⇒ false, payload 就是原 Data); 未实现时沿用旧语义
	// (payload 恒视为 redo 段 ⇒ 重载仍走 Merge, 与修复前结果一致)。
	isRedo := false
	if s.opts.Codec != nil {
		if sc, ok := s.opts.Codec.(RedoShapeCodec); ok {
			base, lsn, payload, isRedo = sc.SplitShape(l.Data)
		} else {
			base, lsn, payload = s.opts.Codec.Split(l)
			isRedo = true
		}
	}
	tmp := *l
	tmp.Data = nil
	raftBuf, err := encodeLogMsgpack(&tmp)
	if err != nil {
		return nil, 0, 0, err
	}
	totalLen := (recHeaderV2 - 4) + len(raftBuf) + len(payload) + recTrailV2
	rec = make([]byte, 4+totalLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totalLen))
	binary.LittleEndian.PutUint16(rec[4:6], formatV2)
	// 记录级形状标志(见 recFlagShapeKnown): 无 codec 时 payload 恒为原 Data,
	// 同样标记为「形状已知 + 非 redo」, 供日后带 codec 重开时原样还原。
	flags := uint16(recFlagShapeKnown)
	if isRedo {
		flags |= recFlagRedo
	}
	binary.LittleEndian.PutUint16(rec[6:8], flags)
	binary.LittleEndian.PutUint64(rec[8:16], base)
	binary.LittleEndian.PutUint64(rec[16:24], lsn)
	binary.LittleEndian.PutUint32(rec[24:28], uint32(len(raftBuf)))
	copy(rec[28:], raftBuf)
	copy(rec[28+len(raftBuf):], payload)
	crc := crc32.Checksum(rec[4:len(rec)-4], castagnoli)
	binary.LittleEndian.PutUint32(rec[len(rec)-4:], crc)
	return rec, base, lsn, nil
}

// encodeV1 构造一条原型期 v1 记录(无 CRC、无 512 填充), 只用于向既有的 v1
// 文件继续追加以免格式混用; 永不新建 v1 文件。
// encodeV1 builds a prototype v1 record (no CRC, no padding) — used only to
// keep appending to existing v1 files without format mixing.
func (s *Store) encodeV1(l *raft.Log) (rec []byte, lsn uint64, err error) {
	payload := l.Data
	var base uint64
	if s.opts.Codec != nil {
		base, lsn, payload = s.opts.Codec.Split(l)
	}
	tmp := *l
	tmp.Data = nil
	raftBuf, err := encodeLogMsgpack(&tmp)
	if err != nil {
		return nil, 0, err
	}
	totalLen := (recHeaderV2 - 4) + len(raftBuf) + len(payload)
	rec = make([]byte, 4+totalLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totalLen))
	binary.LittleEndian.PutUint16(rec[4:6], 1)
	binary.LittleEndian.PutUint64(rec[8:16], base)
	binary.LittleEndian.PutUint64(rec[16:24], lsn)
	binary.LittleEndian.PutUint32(rec[24:28], uint32(len(raftBuf)))
	copy(rec[28:], raftBuf)
	copy(rec[28+len(raftBuf):], payload)
	return rec, lsn, nil
}

// decodeV2 校验并解码一条 v2 记录体(totalLen 之后的字节): 先比 CRC32C, 再按
// raftLen 切出 msgpack 与 payload, 再按记录级 flags 决定还原 Data 的方式:
// 控制命令(shape 已知且非 redo)原样取 payload; redo 段与旧记录(flags==0)
// 交给 codec.Merge 重建业务信封。
// decodeV2 verifies and decodes one v2 record body (bytes after totalLen).
// 算法解读：
//
//	原理：CRC32C(castagnoli) 校验整条记录体, 不符即报错 —— 调用方(scanRecords
//	  的 yield)把它翻成 errScanStop, 于是撕裂/损坏记录成为截断点, 而不是读出
//	  错数据。
//	关键不变量：payload 与 msgpack 段必须各自 copy 出来(append(nil, ...)),
//	  不能引用 raw 缓冲 —— raw 是整文件读入的切片, 长命引用会把整个文件钉在
//	  内存里。
//	代价与边界：raftLen 越界(为负或超出记录体)即拒; 短记录(< 头+尾)即拒。
//	  CRC 是软件表实现, 大 payload 时是纯 CPU 开销。
//	设计权衡：用 CRC32C 而不是更强的摘要, 因为它的目的是抓"撕裂/半写尾"而非
//	  防篡改; Castagnoli 在多平台有硬件指令, 代价最低。
func (s *Store) decodeV2(body []byte) (*raft.Log, uint64, error) {
	if len(body) < recHeaderV2-4+recTrailV2 {
		return nil, 0, errors.New("short v2 record")
	}
	want := binary.LittleEndian.Uint32(body[len(body)-4:])
	got := crc32.Checksum(body[:len(body)-4], castagnoli)
	if want != got {
		return nil, 0, errors.New("crc mismatch")
	}
	raftLen := int(binary.LittleEndian.Uint32(body[20:24]))
	if raftLen < 0 || raftLen > len(body)-(recHeaderV2-4)-recTrailV2 {
		return nil, 0, errors.New("bad raftLen")
	}
	flags := binary.LittleEndian.Uint16(body[2:4])
	base := binary.LittleEndian.Uint64(body[4:12])
	lsn := binary.LittleEndian.Uint64(body[12:20])
	raftBody := body[recHeaderV2-4 : recHeaderV2-4+raftLen]
	payload := body[recHeaderV2-4+raftLen : len(body)-recTrailV2]
	l := &raft.Log{}
	if err := codec.NewDecoderBytes(raftBody, &msgpackHandle).Decode(l); err != nil {
		return nil, 0, err
	}
	switch {
	case s.opts.Codec == nil:
		l.Data = append([]byte(nil), payload...)
	case flags&recFlagShapeKnown != 0 && flags&recFlagRedo == 0:
		// 形状已知且非 redo 段: payload 就是原始 Data(控制命令 checkpoint/
		// lease/catalog)。必须原样返回 —— 走 Merge 会凭空包一层 cmdSingle,
		// FSM 会把控制命令当 redo 追加成垃圾。
		l.Data = append([]byte(nil), payload...)
	default:
		// redo 段, 或旧记录(flags==0, 形状未知): 沿用旧行为, 由 codec 重建
		// 业务信封。旧文件因此保持逐字节可读(redo 正确; 旧控制命令仍是旧
		// 语义, 见 recFlagShapeKnown 注释)。
		l.Data = s.opts.Codec.Merge(base, lsn, append([]byte(nil), payload...))
	}
	return l, lsn, nil
}

// decodeV1 读取一条原型期 v1 记录(无 CRC, 无填充), 只读兼容用 —— 损坏只能靠
// msgpack 解码失败或长度越界发现, 没有校验和兜底。
// decodeV1 reads a prototype v1 record (no CRC) — read-only compatibility.
func decodeV1(body []byte) (*raft.Log, uint64, error) {
	if len(body) < recHeaderV2-4 {
		return nil, 0, errors.New("short v1 record")
	}
	raftLen := int(binary.LittleEndian.Uint32(body[20:24]))
	if raftLen < 0 || raftLen > len(body)-(recHeaderV2-4) {
		return nil, 0, errors.New("bad raftLen")
	}
	lsn := binary.LittleEndian.Uint64(body[12:20])
	l := &raft.Log{}
	if err := codec.NewDecoderBytes(body[recHeaderV2-4:recHeaderV2-4+raftLen], &msgpackHandle).Decode(l); err != nil {
		return nil, 0, err
	}
	l.Data = append([]byte(nil), body[recHeaderV2-4+raftLen:]...)
	return l, lsn, nil
}

// align512 把偏移向上取整到 512 的倍数 —— v2 记录与批尾填充都以 512 为粒度,
// 这既是 O_DIRECT 对偏移/长度对齐的硬要求, 也是零洞判别式的基准。
func align512(off int64) int64 { return (off + 511) &^ 511 }

// reload 读入文件并按自动探测结果重建内存视图(first/logs/offsets/lsnIndex/
// endOff/refSegs/refBases), 最后把游标停在好数据终点 —— 撕尾被下一次追加覆盖。
// ---------------------------------------------------------------------------
// Reload with format auto-detection and torn-tail handling.
//
// Detection order: v2 magic (+ref flag) → v1 sniff (ver field) → legacy. The
// detected format governs both reading AND subsequent appends: a legacy file
// keeps receiving legacy records even when SingleWAL is on (探测回退/在线兼容),
// so an operator can flip the switch without corrupting existing PVCs.
// Ref mode reads redo.log (data authority) first, then the meta file, and
// drops meta entries whose redo bytes were torn away (12.14.3 ③).
// 算法解读：
//
//	原理：探测顺序 = v2 magic("VWAL" + major==2) → v1 嗅探(ver 字段==1) →
//	  legacy。空文件才按开关铺 512B 头; 非空文件一律以文件自身格式为准。
//	  v2-ref 先读 redo.log(数据权威)再读 meta, 丢弃 redo 字节已被撕掉的 meta 条目。
//	关键不变量：格式一旦探明就同时约束"读"与"后续追加" —— 老 legacy 文件在
//	  SingleWAL 打开后仍继续写 legacy 记录, 否则格式混用; first 取首条 index;
//	  v2 的 endOff 恒 512 对齐; 重载末尾必须调 truncateAtGapLocked 把索引空洞
//	  之上的后缀截掉, 否则 LastIndex 与 GetLog 不一致。
//	代价与边界：整文件一次读入内存(readWhole), 大日志下是峰值内存的主要来源;
//	  O_DIRECT 生效时走 readAtAligned 的对齐缓冲读。撕尾只截到最后一个完整
//	  记录, 其后的字节由下一次追加覆盖。
//	设计权衡：格式跟随文件而非跟随开关, 换来"运维翻开关不会毁掉既有 PVC";
//	  代价是同一份代码要同时维护 4 条读写路径。
func (s *Store) reload() error {
	if err := s.f.Sync(); err != nil {
		return err
	}
	info, err := s.f.Stat()
	if err != nil {
		return err
	}
	raw, err := s.readWhole(s.f, info.Size())
	if err != nil {
		return err
	}

	mode := s.opts.resolvedMode()
	if len(raw) == 0 {
		// New file: pick format from the switches and lay down the header(s).
		switch mode {
		case ModeUnified, ModeRef:
			s.format = map[Mode]string{ModeUnified: "v2-singlewal", ModeRef: "v2-ref"}[mode]
			flags := uint16(fileFlagSingleWAL | fileFlagPad512)
			if mode == ModeRef {
				flags |= fileFlagRef
			}
			if err := writeFileHeader(s.f, flags); err != nil {
				return err
			}
			if mode == ModeRef {
				if s.redoF == nil {
					return fmt.Errorf("filestore: ref mode requires redo.log")
				}
				if err := writeFileHeader(s.redoF, fileFlagSingleWAL|fileFlagPad512|fileFlagRef|fileFlagData); err != nil {
					return err
				}
				s.redoEnd = fileHeaderLen
			}
			s.endOff = fileHeaderLen
			_, err := s.f.Seek(fileHeaderLen, io.SeekStart)
			return err
		default:
			s.format = "legacy"
			return nil
		}
	}

	// Existing file: detect.
	start := int64(0)
	s.format = "legacy"
	switch {
	case len(raw) >= fileHeaderLen && string(raw[0:4]) == fileMagic &&
		binary.LittleEndian.Uint16(raw[4:6]) == formatV2:
		flags := binary.LittleEndian.Uint16(raw[6:8])
		if flags&fileFlagRef != 0 {
			s.format = "v2-ref"
		} else {
			s.format = "v2-singlewal"
		}
		start = fileHeaderLen
	case len(raw) >= 6 && binary.LittleEndian.Uint16(raw[4:6]) == 1:
		// v1 prototype: [u32 len][u16 ver=1]... (legacy msgpack never starts
		// with bytes 0x01 0x00 at [4:6] — its body starts with a fixmap).
		s.format = "v1-singlewal"
	}
	if s.format != "legacy" && s.opts.Logger != nil && mode == ModeLegacy {
		s.opts.Logger.Printf("[INFO] filestore: %s is %s, write switch off (format follows file)", s.path, s.format)
	}

	if s.format == "v2-ref" {
		if s.redoF == nil {
			return fmt.Errorf("filestore: %s is v2-ref but redo.log is missing", s.path)
		}
		ri, err := s.redoF.Stat()
		if err != nil {
			return err
		}
		rawRedo, err := s.readWhole(s.redoF, ri.Size())
		if err != nil {
			return err
		}
		if err := s.reloadRef(raw, rawRedo); err != nil {
			return err
		}
		if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
			return err
		}
		if _, err := s.redoF.Seek(s.redoEnd, io.SeekStart); err != nil {
			return err
		}
		return nil
	}

	padBatches := s.format == "v2-singlewal"
	goodEnd, _, err := scanRecords(raw, start, padBatches, func(off int64, body []byte) error {
		var l *raft.Log
		var lsn uint64
		var derr error
		switch s.format {
		case "v2-singlewal":
			l, lsn, derr = s.decodeV2(body)
		case "v1-singlewal":
			l, lsn, derr = decodeV1(body)
		default:
			ll := &raft.Log{}
			derr = codec.NewDecoderBytes(body, &msgpackHandle).Decode(ll)
			l = ll
		}
		if derr != nil {
			return errScanStop // corrupt/torn tail — truncation point
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, off)
		if s.opts.SparseIndex && lsn != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsn] = lsnPos{index: l.Index, off: off}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.endOff = goodEnd
	// 重载后必做: 索引空洞之上的后缀对 raft 不可达, 必须截掉, 否则
	// LastIndex/GetLog 会不一致。
	s.truncateAtGapLocked()
	// Park the cursor at the end of good data; reload reads via ReadAt and
	// never moves it. A torn tail is simply overwritten by the next append.
	if _, err := s.f.Seek(goodEnd, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// truncateAtGapLocked 在重载后发现索引空洞时, 从第一个洞处截断内存视图并把
// meta 文件 ftruncate(+v2 补零对齐), 保证 LastIndex/GetLog 一致。洞以上的后缀
// 对 raft 不可达(processLogs 无法跨过洞 apply), 截断后由领导者按 prevLog 重铺。
// 返回是否发生了截断。调用方须持有 s.mu(或处于 Open 单线程期)。
// 算法解读：
//
//	原理：扫一遍 logs 找第一处 index 不连续(前一条+1 != 本条), 把内存视图切到
//	  洞前, 并对 meta 文件 ftruncate 到洞起点(v2 再补零到 512 对齐), 同步清掉
//	  index 大于保留下限的 lsnIndex 键。
//	关键不变量：洞之上的后缀对 raft 不可达(processLogs 无法跨洞 apply), 故截断
//	  既安全又必要; endOff 必须停在 512 对齐处, 否则下一次追加的记录头不对齐;
//	  此处【不动 unbound 台账】—— 被截掉的 lsn 在绑定时刻已整条清账, 截断前
//	  若有直发重发(重新挂账), 那仍是盘上真实未绑定字节, 由 CompactRedo 回收
//	  其字节时一并清账。
//	代价与边界：O(len(logs) + len(lsnIndex)), 只在重载/追加发现洞时跑; 只处理
//	  第一处洞(其后内容一律丢弃), 因为再往后已无法与 raft 对齐。
//	设计权衡：截断(丢后缀)而不是留洞等领导者补 —— 位置式存储无法表示中间空洞,
//	  留洞会让位置算术命中错误条目(GetLog 的洞检测正是为此设的)。
func (s *Store) truncateAtGapLocked() bool {
	for i := 1; i < len(s.logs); i++ {
		if s.logs[i].Index == s.logs[i-1].Index+1 {
			continue
		}
		cutOff := s.offsets[i]
		if s.opts.Logger != nil {
			s.opts.Logger.Printf("[WARN] filestore: %s 索引空洞 %d→%d —— 重载截断丢弃其上的 %d 条(等领导者重铺)",
				s.path, s.logs[i-1].Index, s.logs[i].Index, len(s.logs)-i)
		}
		s.logs = s.logs[:i]
		s.offsets = s.offsets[:i]
		if s.format == "v2-ref" {
			s.refSegs = s.refSegs[:i]
			s.refBases = s.refBases[:i]
		}
		if err := s.f.Truncate(cutOff); err == nil {
			if s.format == "v2-singlewal" || s.format == "v2-ref" {
				if padded := align512(cutOff); padded > cutOff {
					_, _ = s.f.WriteAt(make([]byte, padded-cutOff), cutOff)
					cutOff = padded
				}
			}
			s.endOff = cutOff
		}
		if s.lsnIndex != nil && len(s.logs) > 0 {
			lastKept := s.logs[len(s.logs)-1].Index
			for lsn, pos := range s.lsnIndex {
				if pos.index > lastKept {
					delete(s.lsnIndex, lsn)
				}
			}
		}
		// J5 泄漏修复(9.24): 此处不动台账 —— 被截掉的 lsn 在绑定时刻已整条
		// 清账; 截断前如有直发重发(重新挂账), 该记录仍是盘上真实未绑定字节,
		// 台账正确保留, 由 CompactRedo 回收其字节时一并清账。
		return true
	}
	return false
}

// writeFileHeader 在偏移 0 写下 512B 的 v2 头(magic + major + flags)随即 Sync
// —— 头必须先于任何记录落盘, 否则重启会把文件判成 legacy。
// writeFileHeader lays down the 512 B v2 header.
func writeFileHeader(f *os.File, flags uint16) error {
	hdr := make([]byte, fileHeaderLen)
	copy(hdr[0:4], fileMagic)
	binary.LittleEndian.PutUint16(hdr[4:6], formatV2)
	binary.LittleEndian.PutUint16(hdr[6:8], flags)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		return err
	}
	return f.Sync()
}

// readWhole 从偏移 0 读满 size 字节; O_DIRECT 生效时走对齐缓冲读, 否则一次
// ReadAt(容忍 io.EOF)。size==0 直接返回 nil。
// readWhole reads size bytes from f at offset 0, honoring O_DIRECT alignment.
func (s *Store) readWhole(f *os.File, size int64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	if s.directIO {
		return readAtAligned(f, 0, int(size))
	}
	raw := make([]byte, size)
	if _, err := f.ReadAt(raw, 0); err != nil && err != io.EOF {
		return nil, err
	}
	return raw, nil
}

// StoreLogs 追加一批日志并只做一次持久化屏障(raft 的持久化契约正是"StoreLogs
// 返回时已在盘上")。落盘前强制连续性护栏, 并把"与既有条目逐字节一致的重叠批"
// 识别为幂等重写吸收掉。
// ---------------------------------------------------------------------------
// StoreLogs: append a batch + one durability barrier (raft's durability
// contract is exactly "on disk when StoreLogs returns").
// 算法解读：
//
//	原理：先过两道连续性检查 —— (a) 批首必须等于 store last+1, 否则可能是洞
//	  或改写; (b) 批内相邻 index 必须连续。(a) 里若 got < want(重叠窗口), 逐条
//	  比对 Index+Term(内容仅在【两侧都是 inline 字节】时才比), 全等即判幂等
//	  重写, 剥掉重叠前缀只追加真正新的尾巴; 整批都是重投则 no-op 返回。
//	关键不变量：护栏必须在【落盘前】炸响 —— 历史上 v2-ref 在高并发 + 快照压实
//	  窗口写出过一个索引洞(75304→75306)且一路静默, 直到 apply GetLog panic 才
//	  暴露。got > want 恒硬拒(真空洞)。
//
// ★三种边界(2026-10-24 补, 40k DDL 档误 stepdown 实证)：
//
//	① batch first <= store last, 且重叠条目 (index,term,内容) 一致
//	   ⇒ 幂等重写(重试批/分批边界/异步写者重派都会产生它 —— 它不是洞),
//	     剥掉重叠前缀、只追加新尾; 整批被覆盖则 no-op 成功。
//	② batch first <= store last, 但同一 index 的 term/内容真不一致
//	   ⇒ 真冲突(日志分叉/内容改写) ⇒ fail-loud, 绝不让它静默覆盖。
//	③ batch first > store last+1 ⇒ 真空洞 ⇒ 保持拒绝。
//
//	「内容一致」的判定必须【形态感知】(见 entryDataForm):
//	  - 两侧都 inline   ⇒ 先经 canonicalEntryData 归一, 再逐字节比。归一化是
//	    必需的: 编解码往返只保证语义等价, 不保证字节相等(单段 cmdBatch 会
//	    归一成 cmdSingle; v2 重载也走 Codec.Merge 重建 Data)。
//	  - 两侧都是 ref redo ⇒ 比【段清单(base + LSN 序列)】这一个编码无关的
//	    语义指纹, 零读盘即可判真冲突。
//	  - 一侧是指针层(省略标记/Data=nil)或已被压实 ⇒ 字节不可比, 只信
//	    (index,term)。
//	  拿指针形态/归一形态直接 bytes.Equal 必然误报, 而误报的代价是
//	  asyncWriteFailed 永久置坏 ⇒ leader step down ⇒ 长征楔死(2026-10-24 现场)。
//
//	代价与边界：重叠窗口的比对是 O(overlap); inline 分支多一次归一化(有 codec
//	  时 split→merge, 无 codec 时零拷贝), ref 分支只比 LSN 列表(不读盘)。
//	  护栏本身是"正常情况永不触发"的检查。
//	设计权衡：宽容幂等重写(而非 fail-closed)是因为行军 40k 档实测到 leader 在
//	  正常任期收到 first==last 的重写批(疑似异步写者重派), 一炸就 asyncWriteFailed
//	  永久置坏 → leader step down → 长征楔死。
func (s *Store) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(logs) == 0 {
		return nil
	}
	// 连续性护栏: raft 契约要求追加严格连续(leader 顺序分配, follower 经
	// prevLog/冲突截断对齐)。历史上 v2-ref 在高并发+快照压实窗口出现过一个
	// 索引空洞(磁盘上 75304 直接跳到 75306)且一路静默, 直到 apply GetLog
	// panic 才暴露 —— 任何不连续都必须在落盘前炸响, 而不是埋成洞。
	if len(s.logs) > 0 {
		if want, got := s.lastIndexLocked()+1, logs[0].Index; got != want {
			// ★幂等重写宽容(2026-10-02 实证: 行军 40k 档 leader 在正常任期中收到
			//   first==last 的重写批 —— 异步写者重派/重试会重发整批; 2026-10-24 又
			//   在 40k DDL 档命中"重叠批 term 相同却被判内容冲突"的误报):
			//   与既有条目【语义一致】的重叠是幂等重写 —— raft 日志匹配原则下
			//   语义 = no-op ⇒ 剥掉重叠只追加新尾。
			//   任一条目在 (index,term,可比内容) 上不一致 ⇒ 真冲突 ⇒ 维持 fail-closed。
			if got < want { // 重写/重叠窗口: first <= last
				overlap := want - got // 重叠条数 = last+1-first
				// 比对口径: Index+Term 恒比; Data 仅在【两侧都是 inline 字节】时比。
				// 见函数头"三种边界": 指针/省略/归一形态的字节比较必然误报, 而误报
				// 会把 leader 无谓地 step down, 所以宁可退到 (index,term) 判据。
				// 实现注: 这里【不能】调 GetLog —— 它持 RLock(本函数已持写锁,
				// Go 互斥锁不可重入)且 ref 路径会做锁移交(交出 s.mu)。
				// peekLogMetaLocked 是纯内存的元数据读取, 无移交。
				allSame := true
				var detail string
				for i := uint64(0); i < overlap && int(i) < len(logs); i++ {
					in := logs[i]
					// 已被压实回收的前缀: raft 只压【已提交】前缀, 这些条目的字节
					// 已从 meta 里物理删掉 —— 无可比, 按幂等 no-op 跳过(不是洞)。
					if s.first != 0 && in.Index < s.first {
						continue
					}
					prev, prevForm, err := s.peekLogMetaLocked(in.Index)
					if err != nil {
						allSame = false
						detail = fmt.Sprintf("stored entry %d unreadable (%v)", in.Index, err)
						break
					}
					if prev.Index != in.Index || prev.Term != in.Term {
						allSame = false
						detail = fmt.Sprintf("index/term conflict at %d: incoming term=%d stored term=%d",
							in.Index, in.Term, prev.Term)
						break
					}
					inForm := s.incomingDataFormLocked(in)
					switch {
					case prevForm == dataFormInline && inForm == dataFormInline:
						// 两侧字节都在各自记录里 ⇒ 归一后逐字节比。
						if !bytes.Equal(s.canonicalEntryData(prev.Data), s.canonicalEntryData(in.Data)) {
							allSame = false
							detail = fmt.Sprintf("content conflict at %d (term=%d): incoming %dB[%s] stored %dB[%s]",
								in.Index, in.Term, len(in.Data), inForm, len(prev.Data), prevForm)
						}
					case prevForm == dataFormRefPointer && inForm == dataFormRefPointer:
						// 两侧都是 ref redo 条目: 字节在 redo.log, 但【段清单
						// (base + LSN 序列)是编码无关的语义指纹】—— 同一逻辑命令
						// 写成批信封还是单条信封, 段清单都一样; 而不同内容必然
						// 出现不同的 base/LSN 或不同段数。零读盘即可判真冲突。
						prevBase, prevLSNs, ok := s.storedRedoSegsLocked(in.Index)
						inBase, inSegs := s.splitEntry(in)
						if !ok || prevBase != inBase || !equalRedoLSNs(prevLSNs, inSegs) {
							allSame = false
							detail = fmt.Sprintf("redo segment conflict at %d (term=%d): incoming base=%d lsns=%v stored base=%d lsns=%v",
								in.Index, in.Term, inBase, redoLSNsOf(inSegs), prevBase, prevLSNs)
						}
					default:
						// 一侧是指针层(elided)/空字节/已被压实: 无可比字节。
						// raft 日志匹配原则下 (index,term) 即唯一判据 ⇒ 幂等放行。
						continue
					}
					if !allSame {
						break
					}
				}
				if allSame {
					if int(overlap) >= len(logs) {
						// 整批都是已落盘内容的幂等重投 ⇒ no-op 成功
						return nil
					}
					// 剥掉重叠前缀, 只追加真正新的尾巴
					logs = logs[overlap:]
				} else {
					return fmt.Errorf("filestore: non-contiguous append: store last=%d, batch first=%d (len=%d) — refusing to write (overlap mismatch: %s)",
						want-1, got, len(logs), detail)
				}
			} else {
				return fmt.Errorf("filestore: non-contiguous append: store last=%d, batch first=%d (len=%d) — refusing to write a hole",
					want-1, got, len(logs))
			}
		}
	}
	for i := 1; i < len(logs); i++ {
		if logs[i].Index != logs[i-1].Index+1 {
			return fmt.Errorf("filestore: non-contiguous batch: [%d]=%d after %d — refusing",
				i, logs[i].Index, logs[i-1].Index)
		}
	}
	if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
		return err
	}

	if s.format == "v2-ref" {
		return s.storeLogsRef(logs)
	}
	if s.format == "v2-singlewal" {
		return s.storeLogsV2(logs)
	}
	if s.format == "v1-singlewal" {
		return s.storeLogsV1(logs)
	}
	return s.storeLogsLegacy(logs)
}

// storeLogsV1 向原型期 v1 文件继续追加 v1 记录(无 CRC、无批尾填充), 逐条 Write
// 后一次 barrier。永不新建 v1 文件。
// storeLogsV1 keeps appending v1-format records to a prototype-era file
// (no CRC, no batch padding). New v1 files are never created.
func (s *Store) storeLogsV1(logs []*raft.Log) error {
	lsns := make([]uint64, len(logs))
	encoded := make([][]byte, len(logs))
	for i, l := range logs {
		rec, lsn, err := s.encodeV1(l)
		if err != nil {
			return err
		}
		lsns[i] = lsn
		encoded[i] = rec
	}
	for i, l := range logs {
		if _, err := s.f.Write(encoded[i]); err != nil {
			return err
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, s.endOff)
		if s.opts.SparseIndex && lsns[i] != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsns[i]] = lsnPos{index: l.Index, off: s.endOff}
		}
		s.endOff += int64(len(encoded[i]))
	}
	return s.barrier()
}

// storeLogsV2 把整批编成 v2 记录、拼接、批尾补零到 512B, 用一次 write + 一次
// barrier(或一次 io_uring enter)持久化 —— Data 字节恰好落盘一次, 这是 12.14
// 的单拷贝点。
// storeLogsV2 encodes the batch as v2 records, concatenates, pads the batch
// tail to 512 B, and persists with one write + one barrier (or one io_uring
// enter). Data bytes land exactly once — this is the 12.14 single-copy point.
// 算法解读：
//
//	原理：编码进 bytes.Buffer → 补零到 512 边界 → (O_DIRECT 时拷进对齐缓冲)
//	  → 优先 io_uring 单 enter 链(WRITE→FSYNC), 失败即回落到 classic Write +
//	  barrier → 最后 commitBatch 推进内存视图。
//	关键不变量：批尾补零长度 = align512(len)-len, 使下一次追加的 baseOff 仍
//	  512 对齐(O_DIRECT 与零洞判别式都依赖它); baseOff 必须在写之前抓拍
//	  (s.endOff), 因为 commitBatch 会改写它; io_uring 失败必须完整回落到经典
//	  路径, 不能半途而废。
//	代价与边界：整批一次内存拷贝(单拷贝点), 批越大越省 syscall 但内存峰值越高;
//	  一批一次 fsync, 批越小 fsync 次数越多(这正是组提交窗口的收益来源)。
//	设计权衡：用"一次写整批 + 一次屏障"而不是逐条写 —— raft 的持久化契约只
//	  要求 StoreLogs 返回时整批在盘上, 逐条 fsync 是纯浪费。
func (s *Store) storeLogsV2(logs []*raft.Log) error {
	lsns := make([]uint64, len(logs))
	var batch bytes.Buffer
	for i, l := range logs {
		rec, _, lsn, err := s.encodeV2(l)
		if err != nil {
			return err
		}
		lsns[i] = lsn
		batch.Write(rec)
	}
	// pad512 的来历: O_DIRECT 要求偏移与长度都按块(512B)对齐, 且零洞判别式
	// 依赖"记录与填充都是 512 对齐"; 故每批尾部补零到下一个 512 边界, 使下
	// 一批的 baseOff 仍对齐。
	if pad := align512(int64(batch.Len())) - int64(batch.Len()); pad > 0 {
		batch.Write(make([]byte, pad))
	}
	buf := batch.Bytes()
	if s.directIO {
		buf = alignedCopy(buf)
	}
	baseOff := s.endOff

	if s.opts.IORing {
		if err := storeLogsIORingChain(s.f, baseOff, [][]byte{buf}, s.opts.IORingSQPoll); err == nil {
			s.ioringOK++
			s.commitBatch(logs, lsns, baseOff, buf)
			return nil
		}
		// fall through to classic write + barrier on any error
	}
	if _, err := s.f.Write(buf); err != nil {
		return err
	}
	if err := s.barrier(); err != nil {
		return err
	}
	s.commitBatch(logs, lsns, baseOff, buf)
	return nil
}

// commitBatch 在批次已持久化后推进内存视图: 逐条追加 logs/offsets, 按记录头的
// 长度字段走完拼接缓冲(DeleteRange 需要按记录粒度的偏移), 按需预分配段并触发
// sync_file_range 回写。调用方须持有 s.mu。
// commitBatch advances the in-memory view after a durable batch write.
// recordOffsets walks the concatenated buffer to keep per-record offsets
// (needed by DeleteRange at record granularity).
func (s *Store) commitBatch(logs []*raft.Log, lsns []uint64, baseOff int64, buf []byte) {
	recOff := int64(0)
	for i, l := range logs {
		if s.first == 0 {
			s.first = l.Index
		}
		recLen := int64(binary.LittleEndian.Uint32(buf[recOff:recOff+4])) + 4
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, baseOff+recOff)
		if s.opts.SparseIndex && lsns[i] != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsns[i]] = lsnPos{index: l.Index, off: baseOff + recOff}
		}
		recOff += recLen
	}
	s.endOff = baseOff + align512(recOff)
	// 预分配与回写踢都是 best-effort: 失败被静默丢弃, 不影响本批的持久化
	// (本批已由上面的 barrier 保证)。
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
	}
	if s.opts.UseSyncFileRange {
		_ = syncFileRange(s.f, 0, 0)
	}
}

// storeLogsLegacy 是上游兼容路径: 每条记录 [u32][msgpack], 无 CRC 无填充; 可选
// 走 io_uring 链, 否则逐条 Write 后一次 barrier。
// storeLogsLegacy is the upstream-compatible path: [u32][msgpack] per record.
func (s *Store) storeLogsLegacy(logs []*raft.Log) error {
	encoded := make([][]byte, len(logs))
	for i, l := range logs {
		body, err := encodeLogMsgpack(l)
		if err != nil {
			return err
		}
		rec := make([]byte, 4+len(body))
		binary.LittleEndian.PutUint32(rec[:4], uint32(len(body)))
		copy(rec[4:], body)
		encoded[i] = rec
	}
	baseOff := s.endOff
	if s.opts.IORing {
		if err := storeLogsIORingChain(s.f, baseOff, encoded, s.opts.IORingSQPoll); err == nil {
			s.ioringOK++
			for i, l := range logs {
				if s.first == 0 {
					s.first = l.Index
				}
				s.logs = append(s.logs, l)
				s.offsets = append(s.offsets, baseOff)
				baseOff += int64(len(encoded[i]))
			}
			s.endOff = baseOff
			if s.opts.PreallocateSegments {
				_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
			}
			if s.opts.UseSyncFileRange {
				_ = syncFileRange(s.f, 0, 0)
			}
			return nil
		}
	}
	for i, l := range logs {
		if _, err := s.f.Write(encoded[i]); err != nil {
			return err
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, s.endOff)
		s.endOff += int64(len(encoded[i]))
	}
	if err := s.barrier(); err != nil {
		return err
	}
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
	}
	if s.opts.UseSyncFileRange {
		_ = syncFileRange(s.f, 0, 0)
	}
	return nil
}

// barrier 是主(meta/unified)文件上的持久化屏障, 转发到 barrierOn(s.f)。
// barrier is the durability barrier on the main (meta/unified) file.
func (s *Store) barrier() error { return s.barrierOn(s.f) }

// barrierOn 按 fd 施加持久化屏障, 三级降级: io_uring FSYNC → fdatasync → fsync;
// 任一级成功即返回, 失败静默往下退(不报错)。
// barrierOn: io_uring FSYNC → fdatasync → fsync, per fd.
func (s *Store) barrierOn(f *os.File) error {
	if s.opts.IORing {
		if err := ioringFsync(f, s.opts.IORingSQPoll); err == nil {
			s.ioringOK++
			return nil
		}
	}
	if s.opts.Fdatasync {
		return fdatasync(f)
	}
	return f.Sync()
}

// StoreLog 是单条追加的便捷包装, 直接转发到 StoreLogs(一次屏障)。
func (s *Store) StoreLog(l *raft.Log) error { return s.StoreLogs([]*raft.Log{l}) }

// GetLog 按 index 取日志写入 l。位置算术命中后仍交叉校验条目 index, 命中别的
// 条目说明中间有洞 —— 绝不安静地把错误条目交出去。ref 模式下条目只存指针,
// 内容从 redo.log 合并重建(复制与 FSM apply 都走这里)。
// 算法解读：
//
//	原理：RLock 内做位置算术 off = index-first, 并用 logs[off].Index == index
//	  交叉验证(洞检测)。ref 条目在同一个 RLock 临界区里抓拍 (base, segLSNs,
//	  位置, fd), 随即持链移交 redoIoMu.RLock(无 TOCTOU, 压实换刀被排在外),
//	  然后放掉 mu 在锁外读盘并合并。
//	关键不变量：抓拍必须在同一临界区完成, 否则压实可能在中途换掉 fd; redo.log
//	  append-only 且落位后不可变(去重重写 = 追加新版本 + 换 map 指针), 这是
//	  "抓拍后可锁外读"的全部依据; 任何错误路径都必须放掉已持有的锁(mu 或
//	  redoIoMu), 否则整进程死锁。
//	代价与边界：命中洞时返回包着 ErrLogNotFound 的错误(调用方仍可用 errors.Is
//	  判定); ref 条目一次 GetLog = 一次 redo.log ReadAt + 一次 Merge, 比 inline
//	  条目贵。
//	设计权衡：读锁分离是为了让 apply 路径的 mergeEntry 重建读不再与 StoreLogs
//	  的 fsync/直发写在大锁里互挤(实测约 -20% 吞吐的税)。
func (s *Store) GetLog(index uint64, l *raft.Log) error {
	s.mu.RLock()
	if s.first == 0 || index < s.first {
		s.mu.RUnlock()
		return raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		s.mu.RUnlock()
		return raft.ErrLogNotFound
	}
	if got := s.logs[off].Index; got != index {
		// 位置算术命中了别的条目 —— 说明中间有洞(不连续追加)。绝不可把
		// 错误条目静默交给调用方(raft 会拿它做 prevLog 校验/apply)。
		s.mu.RUnlock()
		return fmt.Errorf("filestore: hole detected: GetLog(%d) maps to entry %d (first=%d len=%d): %w",
			index, got, s.first, len(s.logs), raft.ErrLogNotFound)
	}
	*l = *s.logs[off]
	// ref mode: redo entries store only the pointer; reconstruct Data from
	// redo.log (raft replication and FSM apply both read through here).
	// 12.14 读锁分离: 抓拍 (base, segLSNs, 位置, fd) 在同一个 RLock 临界区内
	// 完成并持链移交 redoIoMu.RLock(无 TOCTOU, 压实换刀被排在外), 盘读与
	// 合并放锁外 —— redo.log append-only 不可变。
	if s.format == "v2-ref" && off < uint64(len(s.refSegs)) && len(s.refSegs[off]) > 0 {
		base := s.refBases[off]
		segLSNs := append([]uint64(nil), s.refSegs[off]...)
		f, poss, err := s.pinRefReadLocked(segLSNs)
		if err != nil {
			return err // mu 已放
		}
		data, err := s.mergeCaptured(f, base, segLSNs, poss)
		s.redoIoMu.RUnlock()
		if err != nil {
			return err
		}
		l.Data = data
		return nil
	}
	s.mu.RUnlock()
	return nil
}

// FirstIndex 返回日志中最小(最旧)的 index, 空日志返回 0。持 RLock。
func (s *Store) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.first, nil
}

// entryDataForm 描述一条条目的 Data 在【比较时】处于什么形态 —— 决定重叠
// 幂等校验能不能做字节比较。字节比较只在两端都是 inline 时才有意义:
//   - ref 指针形态的字节在 redo.log(内存视图 Data=nil), 展开需要读盘与锁移交;
//   - 省略标记形态(Extensions 带 elide marker)的 Data 被摘掉, 是"指针层"副本;
//   - 空 Data 没有任何可比字节。
//
// 这几种形态拿 bytes.Equal 去比必然得出"内容不一致", 而误报的代价是
// asyncWriteFailed 永久置坏 ⇒ leader step down(2026-10-24 40k DDL 档实证)。
type entryDataForm uint8

const (
	// dataFormNoBytes: 该侧没有可比字节(空 Data / 已被压实 / 读取失败)。
	dataFormNoBytes entryDataForm = iota
	// dataFormRefPointer: ref 模式 redo 条目 —— Data 存 redo.log, 内存视图为 nil。
	dataFormRefPointer
	// dataFormElided: 指针层条目(Extensions 带 elide marker), Data 已省略。
	dataFormElided
	// dataFormInline: Data 就在本条记录里, 可与同为 inline 的一侧做内容比较。
	dataFormInline
)

// String 供重叠冲突日志直出形态, 避免下次又把形态差异误读成 term 冲突。
func (f entryDataForm) String() string {
	switch f {
	case dataFormInline:
		return "inline"
	case dataFormRefPointer:
		return "ref-pointer"
	case dataFormElided:
		return "elided-pointer"
	default:
		return "no-bytes"
	}
}

// peekLogMetaLocked 读回既有条目的【内存态】元数据(已持 s.mu, 读或写)。
// 与 GetLog 的区别: 不做 ref 模式的 redo.log 合并(那会走 redoIoMu 移交,
// 持写锁者不可用) —— ref 条目返回 Data=nil 且形态为 dataFormRefPointer。
// 供 StoreLogs 的幂等重写比对用。
func (s *Store) peekLogMetaLocked(index uint64) (raft.Log, entryDataForm, error) {
	if s.first == 0 || index < s.first {
		return raft.Log{}, dataFormNoBytes, raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		return raft.Log{}, dataFormNoBytes, raft.ErrLogNotFound
	}
	if got := s.logs[off].Index; got != index {
		return raft.Log{}, dataFormNoBytes, fmt.Errorf("filestore: hole detected: peekLogMeta(%d) maps to entry %d (first=%d len=%d): %w",
			index, got, s.first, len(s.logs), raft.ErrLogNotFound)
	}
	l := *s.logs[off]
	if s.format == "v2-ref" && off < uint64(len(s.refSegs)) && len(s.refSegs[off]) > 0 {
		l.Data = nil // ref 指针形态: 内容在 redo.log, 此处不展开
		return l, dataFormRefPointer, nil
	}
	if len(l.Data) == 0 {
		return l, dataFormNoBytes, nil
	}
	return l, dataFormInline, nil
}

// storedRedoSegsLocked 返回已落盘 ref 条目的段清单(base + LSN 序列)。
// 段清单是【编码无关】的语义指纹: 同一逻辑命令无论写成 cmdBatch(count=1) 还是
// 归一后的 cmdSingle, base 与 LSN 序列都相同, 而字节序列不一定相同; 反过来,
// 内容不同的两条命令不可能给出相同的 base+LSN 序列(redo 流按 LSN 定位)。
// 因此它让 ref 模式在不读盘、不做字节比较的前提下仍能识别真冲突。
// 调用方须持 s.mu。
func (s *Store) storedRedoSegsLocked(index uint64) (uint64, []uint64, bool) {
	if s.format != "v2-ref" || s.first == 0 || index < s.first {
		return 0, nil, false
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) || off >= uint64(len(s.refSegs)) || len(s.refSegs[off]) == 0 {
		return 0, nil, false
	}
	var base uint64
	if off < uint64(len(s.refBases)) {
		base = s.refBases[off]
	}
	return base, s.refSegs[off], true
}

// equalRedoLSNs 比较两条段清单的 LSN 序列(顺序即拼接顺序, 必须逐位相等)。
func equalRedoLSNs(stored []uint64, in []RedoSegment) bool {
	if len(stored) != len(in) {
		return false
	}
	for i := range stored {
		if stored[i] != in[i].LSN {
			return false
		}
	}
	return true
}

// redoLSNsOf 抽出段清单里的 LSN 序列, 仅用于冲突日志。
func redoLSNsOf(segs []RedoSegment) []uint64 {
	if len(segs) == 0 {
		return nil
	}
	out := make([]uint64, len(segs))
	for i := range segs {
		out[i] = segs[i].LSN
	}
	return out
}

// incomingDataFormLocked 判定【待写入条目】的 Data 形态是否可与已落盘条目
// 逐字节比较。调用方须持 s.mu。
//
//	空 Data            ⇒ 无可比字节(可能是尚未展开的指针形态);
//	Extensions 有省略标记 ⇒ 指针层副本, 字节不可比;
//	v2-ref 且会被切成 redo 段 ⇒ 本次写入的字节落在 redo.log, 与内存视图不同源;
//	其余               ⇒ inline, 可比较。
func (s *Store) incomingDataFormLocked(l *raft.Log) entryDataForm {
	if len(l.Data) == 0 {
		return dataFormNoBytes
	}
	if raft.DecodeElideMarker(l.Extensions) != nil {
		return dataFormElided
	}
	if s.format == "v2-ref" {
		if _, segs := s.splitEntry(l); len(segs) > 0 {
			return dataFormRefPointer
		}
	}
	return dataFormInline
}

// canonicalEntryData 把 inline 字节归一成【语义形态】后再比较。
//
// codec 的编解码往返只承诺语义等价, 不承诺字节相等: 例如单段 cmdBatch(count=1)
// 经 SplitSegments→MergeSegments 会归一成 cmdSingle(LogDB 的 logdbRedoCodec 注释
// 明写"cmdBatch(count=1) 归一为 cmdSingle"), v2 文件重载时 decodeV2 也会走
// Codec.Merge 重建 Data。于是【同一 index/term 的两条记录字节不同却完全合法】。
// 直接 bytes.Equal 会把这种合法形态判成内容冲突 ⇒ 无谓 stepdown。
// 归一化: 有 RedoSegmentCodec 且能切出 ≥1 段时, 用 MergeSegments 重建;
// 切不出段(非 redo 命令行)则原字节即语义, 保持不变。
func (s *Store) canonicalEntryData(data []byte) []byte {
	if len(data) == 0 || s.opts.Codec == nil {
		return data
	}
	sc, ok := s.opts.Codec.(RedoSegmentCodec)
	if !ok {
		return data
	}
	base, segs := sc.SplitSegments(data)
	if len(segs) == 0 {
		return data
	}
	return sc.MergeSegments(base, segs)
}

// LastIndex 返回日志中最大(最新)的 index, 空日志返回 0。持 RLock。
func (s *Store) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked(), nil
}

// DeleteRange 删除闭区间 [min, max] 的条目, 区间外的条目必须存活(raft 契约:
// 快照压实删前缀 [first, snapIdx-trailing], 冲突截断删后缀 [conflict, last])。
//
// 实现分两路:
//   - 后缀删除(min > first): 文件直接截断到 min 的记录偏移, 廉价。
//   - 前缀/整段删除(min <= first): 追加式文件无法原地挖洞 —— 内存丢弃 +
//     幸存者记录原字节打包重写(tmp+rename+fsync dir, 与 CompactRedo 同模式)。
//     历史上这里错误地复用了截断路径: keep=min-first=0 → Truncate(offsets[0])
//     → 把整个日志(包括压实必须保留的后缀)全部抹掉, 只能靠 InstallSnapshot
//     自愈, 且在 apply 与压实交叠时直接 panic("log not found")。
//
// Ref 模式: 只动 meta 文件 —— LSN 不随 index 单调(批量提交折叠不连续段),
// redo.log 字节绝不中段截断; 孤儿 redo 空间由 CompactRedo 在检查点回收。
// v2 格式截断/重写后补零到 512B 边界, 使下一批次保持 512 对齐(O_DIRECT 安全)。
func (s *Store) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) == 0 || min > max || max < s.first {
		return nil
	}
	last := s.lastIndexLocked()

	// ★#106(2026-09-26): min > last 时**无事可做** —— 要删的区间整个在日志之后。
	// 与上面 `max < s.first`(区间整个在日志之前)对称, 同样返回 nil。
	//
	// 这条守卫同时堵掉一个**越界读**: 下面是位置算术 keep = min - first, 当
	// min > last 时 keep > len(logs)。原代码在这里写了 `if keep > len(s.logs) { keep = len(s.logs) }`
	// —— 它只把**切片上界**夹住了, 紧接着的 `truncAt := s.offsets[keep]` 仍用夹住后的
	// 值索引, 于是 offsets[len] 越界 panic(index out of range [20] with length 20,
	// 复现: 建 [10,29] 后 DeleteRange(40,45))。那个 clamp 是**假保护**: 它让这行
	// 看起来被保护了, 实际上漏掉了真正会被索引的那一个。
	//
	// 走的是公共接口(LogStore.DeleteRange), 参数又来自外部 —— 一旦越界就是整个
	// 存储进程被 panic 带走, 与"一次非法删除"的代价完全不成比例。
	if min > last {
		return nil
	}

	// 后缀删除(冲突截断): 现有廉价截断路径。
	if min > s.first {
		// 由上面的 min <= last 得 keep = min-first <= last-first = len-1,
		// 故 offsets[keep] 必定在界内(不再需要 clamp —— 见上)。
		keep := int(min - s.first)
		// raft 语义上 max==last; 若调用方给了更小的 max(中段删除 —— raft 的
		// 两个调用点(压实/冲突)都不会这样用), 防御性回落到打包重写, 绝不误删
		// (max, last] 的存活条目。
		if max < last {
			return s.deleteRangeRewriteLocked(min, max)
		}
		truncAt := s.offsets[keep]
		s.logs = append([]*raft.Log(nil), s.logs[:keep]...)
		s.offsets = append([]int64(nil), s.offsets[:keep]...)
		if s.format == "v2-ref" {
			s.refSegs = append([][]uint64(nil), s.refSegs[:keep]...)
			s.refBases = append([]uint64(nil), s.refBases[:keep]...)
		}
		if len(s.logs) == 0 {
			s.first = 0
		}
		if s.lsnIndex != nil {
			for lsn, pos := range s.lsnIndex {
				if pos.index >= min {
					delete(s.lsnIndex, lsn)
				}
			}
		}
		oldEnd := s.endOff
		if err := s.f.Truncate(truncAt); err != nil {
			return err
		}
		s.endOff = truncAt
		if s.format == "v2-singlewal" || s.format == "v2-ref" {
			if padded := align512(truncAt); padded > truncAt {
				if _, err := s.f.WriteAt(make([]byte, padded-truncAt), truncAt); err != nil {
					return err
				}
				s.endOff = padded
			}
		}
		if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
			return err
		}
		if err := s.barrier(); err != nil {
			return err
		}
		if s.opts.UseFadvise && oldEnd > s.endOff {
			_ = fadviseDontNeed(s.f, s.endOff, oldEnd-s.endOff)
		}
		return nil
	}

	// 前缀/整段删除(min <= first): 幸存者 = (max, last]。
	return s.deleteRangeRewriteLocked(min, max)
}

// deleteRangeRewriteLocked 处理所有 min <= s.first 的删除: 把 [max,last] 外的
// 幸存者记录原字节打包重写为新 meta 文件并原子 rename。调用方须持有 s.mu。
// 算法解读：
//
//	原理：max >= last 走整段删除(截回起始偏移即可); 否则把幸存者 (max, last]
//	  的记录【原字节】读出、拼进 bytes.Buffer(每满 1MiB flush 一次)、写进
//	  path+".compact.tmp", Sync + Close 后 os.Rename 原子换名, 再 syncDir,
//	  最后以 openDirectFlag(s.directIO) 重开并重算 offsets/first/endOff。
//	关键不变量：rename 成功之前绝不关旧句柄(失败则旧文件仍是权威); v2 下头部
//	  512B 原样抄过去(保留 flags), 每段 flush 都补零到 512 边界; 新 offsets
//	  必须按"本段写入起点 + 缓冲内偏移"重算, 不能沿用旧偏移; lsnIndex 只删
//	  index 落在 [min,max] 的键, 幸存者的绑定保留。
//	代价与边界：O(幸存者字节) 的重写 + 一次全量 rename, 是最贵的一条路径 ——
//	  所以只在 min <= first 的前缀删除(快照压实)时才走; 内存靠 1MiB 分段 flush
//	  封顶; drop <= 0 或 drop >= len(logs) 直接 no-op。
//	设计权衡：追加式文件无法原地挖洞, 而 raft 的压实又是常态操作, 于是选
//	  "打包重写 + 原子 rename"而不是"墓碑 + 定期 GC" —— 代价是压实瞬间的写
//	  放大, 换来读路径永远不必跳墓碑、文件永远紧凑。
func (s *Store) deleteRangeRewriteLocked(min, max uint64) error {
	last := s.lastIndexLocked()
	if max >= last {
		// 整段删除(RecoverCluster): 截回起始偏移即可。
		truncAt := int64(0)
		if s.format == "v2-singlewal" || s.format == "v2-ref" || s.format == "v1-singlewal" {
			truncAt = s.offsets[0] // v2=header 之后; v1/legacy=0
		}
		s.logs = nil
		s.offsets = nil
		if s.format == "v2-ref" {
			s.refSegs = nil
			s.refBases = nil
		}
		s.first = 0
		s.lsnIndex = nil
		if err := s.f.Truncate(truncAt); err != nil {
			return err
		}
		s.endOff = truncAt
		if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
			return err
		}
		return s.barrier()
	}

	drop := int(max - s.first + 1) // 删除 [first, max], 保留 s.logs[drop:]
	if drop <= 0 || drop >= len(s.logs) {
		return nil
	}
	isV2 := s.format == "v2-singlewal" || s.format == "v2-ref"

	dir := filepath.Dir(s.path)
	tmpPath := s.path + ".compact.tmp"
	nf, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	writeOff := int64(0)
	if isV2 {
		hdr := make([]byte, fileHeaderLen)
		if _, err := s.f.ReadAt(hdr, 0); err != nil { // 原样保留头部(flags)
			nf.Close()
			return err
		}
		if _, err := nf.WriteAt(hdr, 0); err != nil {
			nf.Close()
			return err
		}
		writeOff = fileHeaderLen
	}
	var batch bytes.Buffer
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		if isV2 {
			if pad := align512(int64(batch.Len())) - int64(batch.Len()); pad > 0 {
				batch.Write(make([]byte, pad))
			}
		}
		buf := batch.Bytes()
		if s.directIO {
			buf = alignedCopy(buf)
		}
		if _, err := nf.WriteAt(buf, writeOff); err != nil {
			return err
		}
		writeOff += int64(batch.Len())
		batch.Reset()
		return nil
	}
	newOffsets := make([]int64, 0, len(s.logs)-drop)
	for i := drop; i < len(s.logs); i++ {
		var lenBuf [4]byte
		if _, err := s.f.ReadAt(lenBuf[:], s.offsets[i]); err != nil {
			nf.Close()
			return err
		}
		recLen := 4 + int(binary.LittleEndian.Uint32(lenBuf[:]))
		raw := make([]byte, recLen)
		if _, err := s.f.ReadAt(raw, s.offsets[i]); err != nil {
			nf.Close()
			return err
		}
		newOffsets = append(newOffsets, writeOff+int64(batch.Len()))
		batch.Write(raw)
		if batch.Len() >= 1<<20 {
			if err := flush(); err != nil {
				nf.Close()
				return err
			}
		}
	}
	if err := flush(); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Close(); err != nil {
		return err
	}
	// rename 成功前绝不关旧句柄(失败则旧文件仍是权威)。
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	// rename 之后必须 fsync 目录, 否则崩溃后新文件名可能尚未落地(旧文件已
	// 被换名, 目录项丢失 = 整个日志消失)。syncDir 内部吞掉错误。
	syncDir(dir)
	newF, err := os.OpenFile(s.path, os.O_RDWR|openDirectFlag(s.directIO), 0o644)
	if err != nil {
		return err
	}
	_ = s.f.Close()
	s.f = newF
	s.logs = append([]*raft.Log(nil), s.logs[drop:]...)
	s.offsets = newOffsets
	if s.format == "v2-ref" {
		s.refSegs = append([][]uint64(nil), s.refSegs[drop:]...)
		s.refBases = append([]uint64(nil), s.refBases[drop:]...)
	}
	s.first = s.logs[0].Index
	s.endOff = writeOff
	if s.lsnIndex != nil {
		for lsn, pos := range s.lsnIndex {
			if pos.index >= min && pos.index <= max {
				delete(s.lsnIndex, lsn)
			}
		}
	}
	if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
		return err
	}
	if s.opts.PreallocateSegments {
		_ = preallocate(s.f, s.endOff, s.opts.SegmentSize)
	}
	return nil
}

// GetLogRaw 返回 index 对应的原始 Data 字节(12.8.2 raw-bytes 路径, 不经过
// msgpack 解码); ref 模式与 GetLog 同走"抓拍 + 锁外读盘合并"两阶段。
// GetLogRaw returns the raw Data bytes for index (12.8.2 raw-bytes path).
func (s *Store) GetLogRaw(index uint64) ([]byte, error) {
	s.mu.RLock()
	if s.first == 0 || index < s.first {
		s.mu.RUnlock()
		return nil, raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		s.mu.RUnlock()
		return nil, raft.ErrLogNotFound
	}
	if s.format == "v2-ref" && off < uint64(len(s.refSegs)) && len(s.refSegs[off]) > 0 {
		base := s.refBases[off]
		segLSNs := append([]uint64(nil), s.refSegs[off]...)
		f, poss, err := s.pinRefReadLocked(segLSNs)
		if err != nil {
			return nil, err // mu 已放
		}
		data, err := s.mergeCaptured(f, base, segLSNs, poss)
		s.redoIoMu.RUnlock()
		return data, err
	}
	data := append([]byte(nil), s.logs[off].Data...)
	s.mu.RUnlock()
	return data, nil
}

// LookupByLSN 用稀疏索引把 redo LSN 解析成 raft index。索引在两种情况下建立:
// unified/v1 + SparseIndex 开关, 或 ref 模式(无条件 —— 它就是读路径)。
// LookupByLSN resolves a redo LSN to its raft index via the sparse index.
// The index is populated when: unified/v1 + SparseIndex switch, or ref mode
// (unconditionally — it is the read path).
// 算法解读：
//
//	原理：map[lsn]lsnPos{index, off}, 绑定时刻 = 该 lsn 第一次被某条 raft meta
//	  引用时(建 lsnIndex, 或 commitBatch 里填表)。绑定域即"raft 已提交可见的
//	  LSN 集合", 与直发台账域(unboundPending)对照即可定位泄漏。
//	关键不变量：lsn==0 表示"无 LSN"(codec 未拆出 envelope), 一律不入索引 ——
//	  否则大量条目会挤在 0 这一个键上互相覆盖; 索引是【稀疏】的, 查不到 ≠ 数据
//	  不在盘上(可能只是尚未被任何 meta 引用)。
//	代价与边界：O(1) 查表, 内存 = O(已绑定条目数) 的 map 开销; 删日志时必须
//	  同步清键(DeleteRange 两条路径都做了), 否则索引会随压实次数单调涨。
//	设计权衡：用 map 而不是有序数组 + 二分, 因为 LSN 由业务分配、不保证单调,
//	  二分要求有序会引入额外的排序维护成本。
func (s *Store) LookupByLSN(lsn uint64) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lsnIndex == nil {
		return 0, false
	}
	pos, ok := s.lsnIndex[lsn]
	if !ok {
		return 0, false
	}
	return pos.index, true
}

// Stats is an observability snapshot for /storeinfo-style endpoints.
type Stats struct {
	Format        string `json:"format"`
	SparseIndex   bool   `json:"sparseIndex"`
	LSNEntries    int    `json:"lsnEntries"`
	FirstIndex    uint64 `json:"firstIndex"`
	LastIndex     uint64 `json:"lastIndex"`
	EndOffset     int64  `json:"endOffset"`
	RedoEndOffset int64  `json:"redoEndOffset,omitempty"`
	IORing        bool   `json:"ioRing"`
	IORingOK      uint64 `json:"ioRingOK"` // chains actually completed (probe evidence)
	DirectIO      bool   `json:"directIO"`

	RedoHealedHoles uint64 `json:"redoHealedHoles,omitempty"` // #163: zero holes skipped on reload
	RedoHoleBytes   int64  `json:"redoHoleBytes,omitempty"`   // #163: bytes covered by healed holes

	// J5 绑定速率观测(进程累计计数, 速率由读方取窗口差分)。
	RedoDirectSegs   uint64 `json:"redoDirectSegs,omitempty"`   // WriteRedoDirect 成功发布段数
	RedoBoundSegs    uint64 `json:"redoBoundSegs,omitempty"`    // raft meta 首次绑定段数(建 lsnIndex 时刻)
	RedoBoundBytes   int64  `json:"redoBoundBytes,omitempty"`   // 绑定段承载的负载字节数
	RedoUnboundBytes int64  `json:"redoUnboundBytes,omitempty"` // P3 未绑定水位(当前值 gauge)
}

// unboundReserve 登记一条直发预留 (lsn, n 字节), 同时加台账与现金水位; n<=0
// 直接 no-op。同 LSN 多代共存时各代各自挂账, 现金是全部代之和。
// J5 泄漏修复(9.24): 未绑定水位的配对记账。生命周期按事件驱动:
//
//	① reserve(lsn, n): WriteRedoDirect 阶段1 每次"新代"预留登记(直发重写
//	  场景同 LSN 多代共存, 各代各自挂账, 现金是全部代之和)。
//	② 终点事件即清账: 绑定(storeLogsRef 建 lsnIndex)、重写替代(direct/
//	  storeLogsRef 发布新代前)、压实丢弃(lsn<keepFrom) —— 任一终点发生
//	  即 unboundDrain 整条清账(旧代+新代), 后续再投递 no-op。
//	③ 失败即回滚: 占位/写盘失败按本次预留量 unboundRelease 退还。
//	不变式: 现金 unboundBytes 恒 = Σ台账, Stats 照旧读现金。
//
// 台账随进程存亡: reloadRef 从 redo 扫描重建(等价于把盘上未绑定记录重新
// reserve); DeleteRange 清 lsnIndex 时一并清台账; CompactRedo 换 map 时
// 同步清掉丢弃者的台账。
// 算法解读：
//
//	原理：现金 unboundBytes 与台账 unboundPending[lsn] 恒等 —— reserve 加,
//	  release/drain 减, Stats 只读现金。生命周期按事件驱动: ①reserve 挂账;
//	  ②终点事件(绑定/重写替代/压实丢弃)整条清账; ③失败按本次预留量退还。
//	关键不变量：现金恒 = Σ台账(破坏它 → 水位虚高单调涨满 UnboundRedoLimit →
//	  直发通道永久背压); 台账随进程存亡(reloadRef 从 redo 扫描重建, DeleteRange
//	  清 lsnIndex 时一并清台账, CompactRedo 换 map 时同步清); 现金扣减只在
//	  "有台账"时发生 —— 这是防双扣的唯一闸门(K6 实测的 9443 次 bound=true
//	  重写再投递就靠它吸收)。
//	代价与边界：map 操作 O(1), 内存 = O(存活 LSN 数); 没有 TTL 也没有上限,
//	  泄漏是单调累积的(K6 实测 60% 预留泄漏, 268MiB 钉死)。
//	设计权衡：用 per-LSN 台账而不是只记总额, 是为了让"哪条 LSN 泄漏"可诊断
//	  (DiagUnboundLsns/DiagUnboundAudit), 代价是每个 LSN 一份 map 条目。
func (s *Store) unboundReserve(lsn uint64, n int64) {
	if n <= 0 {
		return
	}
	if s.unboundPending == nil {
		s.unboundPending = make(map[uint64]int64)
	}
	s.unboundPending[lsn] += n
	s.unboundBytes += n
}

// unboundRelease 释放至多 n 字节。台账不存在(已绑定后的重发、raft 原生段
// 的绑定、压实后的残留分支)整体 no-op —— 防双扣的唯一闸门: 现金扣减只在
// 有台账时发生, K6 实测的 9443 次 bound=true 重写再投递在这里被吸收。
func (s *Store) unboundRelease(lsn uint64, n int64) {
	if n <= 0 {
		return
	}
	p, ok := s.unboundPending[lsn]
	if !ok {
		return
	}
	if p > n {
		s.unboundPending[lsn] = p - n
	} else {
		delete(s.unboundPending, lsn)
	}
	s.unboundBytes -= n
	if s.unboundBytes < 0 {
		s.unboundBytes = 0
	}
}

// unboundDrain 清掉某 LSN 的整条台账并从现金扣除。多代共存时台账可能
// 大于当前 redoMap 记录的长度(每代各自 reserve, 旧代等在账上) —— 按事件
// 消费: 绑定/替代/压实丢弃任一终点事件发生即整条清账, 只释放 pos.length
// 会留下旧代残账(又一处泄漏)。终点之后同 LSN 的再投递(直发重发、重绑定)
// 全部 no-op。
func (s *Store) unboundDrain(lsn uint64) {
	p, ok := s.unboundPending[lsn]
	if !ok {
		return
	}
	delete(s.unboundPending, lsn)
	s.unboundBytes -= p
	if s.unboundBytes < 0 {
		s.unboundBytes = 0
	}
}

// Stats 返回给 /storeinfo 一类端点用的观测快照: 格式、索引规模、首尾 index、
// 文件偏移、io_uring/O_DIRECT 生效状态、空洞治愈计数与 J5 绑定速率计数器。
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		Format:          s.format,
		SparseIndex:     s.opts.SparseIndex,
		LSNEntries:      len(s.lsnIndex),
		FirstIndex:      s.first,
		LastIndex:       s.lastIndexLocked(),
		EndOffset:       s.endOff,
		RedoEndOffset:   s.redoEnd,
		IORing:          s.opts.IORing,
		IORingOK:        s.ioringOK,
		DirectIO:        s.directIO,
		RedoHealedHoles: s.redoHealedHoles,
		RedoHoleBytes:   s.redoHoleBytes,

		RedoDirectSegs:   s.directRedoSegs,
		RedoBoundSegs:    s.boundRedoSegs,
		RedoBoundBytes:   s.boundRedoBytes,
		RedoUnboundBytes: s.unboundBytes,
	}
}

// DiagUnboundLsns 是只读诊断(J5 泄漏定位): 返回台账里的 lsn 有序样本。
func (s *Store) DiagUnboundLsns() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint64, 0, len(s.unboundPending))
	for lsn := range s.unboundPending {
		out = append(out, lsn)
	}
	return out
}

// DiagLsnIndexStats 返回 lsnIndex(绑定域)的 (n, min, max) —— 诊断: 对比
// raft 绑定域与直发台账域是否同域。
func (s *Store) DiagLsnIndexStats() (n int, min, max uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	min, max = ^uint64(0), 0
	for lsn := range s.lsnIndex {
		n++
		if lsn < min {
			min = lsn
		}
		if lsn > max {
			max = lsn
		}
	}
	return n, min, max
}

// DiagLsnIndex 查询某 lsn 是否已绑定(在 lsnIndex 里)及其引用的 raft index。
func (s *Store) DiagLsnIndex(lsn uint64) (bound bool, index uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.lsnIndex[lsn]; ok {
		return true, p.index
	}
	return false, 0
}

// DiagRedoLsnSample 返回 redoMap 里的 lsn 有序样本(诊断: 对比直发域与绑定域)。
func (s *Store) DiagRedoLsnSample(limit int) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint64, 0, limit)
	for lsn := range s.redoMap {
		out = append(out, lsn)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// DiagUnboundAudit 是 J5 泄漏的精确对账(只读诊断)。盘上未绑定记录分三类,
// 现金只对第一类负责:
//
//	ledgered = 台账在册且盘上有字节 —— 正常绑定滞后, cash 应 = Σ其记录全长。
//	  盘上字节比台账短的部分(同 LSN 更短新代已占盘)计入 over(多记账, 现金
//	  按旧代全长持有而盘上已无该字节)。
//	missing  = 盘上有字节、未绑定(lsnIndex 无)、台账无账 —— 残留类。来源:
//	  DeleteRange(快照截断/日志压实)清 lsnIndex 绑定而不清 redo 字节; 兜底
//	  分支(重载时误判已绑定)。现金无需对它负责 —— 该类字节会在下一次
//	  CompactRedo 时被 GC(keepFrom 之下)并同步清账, 或重启 reloadRef 重新
//	  reserve 归入正常类。>0 恒表示"有等待回收的化石", 不漂移现金。
//	orphan   = 台账在册而 redoMap 已无字节(账悬空) —— 严格异常, 计数。
//
// 离线重扫口径(全盘未绑定) = ledgeredDisk + missing; 现金对账恒等式:
//
//	cash == Σ(ledgered 记录全长) - over  (多代残账经终点事件清账后应恒成立)。
func (s *Store) DiagUnboundAudit() (cash, ledgeredDisk, over, missing int64, orphanLsns []uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cash = s.unboundBytes
	for lsn, p := range s.unboundPending {
		pos, ok := s.redoMap[lsn]
		if !ok {
			orphanLsns = append(orphanLsns, lsn)
			continue
		}
		ledgeredDisk += int64(pos.length)
		if int64(pos.length) > p {
			over += int64(pos.length) - p
		}
	}
	for lsn, pos := range s.redoMap {
		if _, bound := s.lsnIndex[lsn]; bound {
			continue
		}
		if _, inLedger := s.unboundPending[lsn]; !inLedger {
			missing += int64(pos.length)
		}
	}
	return cash, ledgeredDisk, over, missing, orphanLsns
}

// DiagSplitEntry 按 store 自带 codec 解析一条 raft 条目的 Data, 返回它引用的
// (base, 段 LSNs) —— 诊断: 对比 raft 绑定域与直发台账域是否同域。
func (s *Store) DiagSplitEntry(data []byte) (uint64, []uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	base, segs := s.splitEntry(&raft.Log{Data: data})
	lsns := make([]uint64, len(segs))
	for i, seg := range segs {
		lsns[i] = seg.LSN
	}
	return base, lsns
}

// ---------------------------------------------------------------------------
// Ref-mode business read API (12.14.3): the redo view of the store.

// LookupRedo 把 lsn 解析成 redo.log 里的 (offset, length), 命中与否由 ok 给出
// —— 读的是内存 presence map, 不碰盘。持 RLock。
// LookupRedo resolves an lsn to its redo.log offset/length (presence map).
func (s *Store) LookupRedo(lsn uint64) (off int64, length int, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pos, ok := s.redoMap[lsn]
	return pos.off, pos.length, ok
}

// ReadRedo 返回某个 lsn 的 redo 负载(未按 framer 解帧、未 Merge)。两阶段: RLock
// 抓拍位置 + fd, 持链移交 redoIoMu.RLock 后锁外读盘; 读失败静默返回 false。
// ReadRedo returns the redo payload for an lsn (unframed, pre-Merge).
// 两阶段: mu.RLock 抓拍位置+fd, 持链移交 redoIoMu.RLock 后锁外读盘。
func (s *Store) ReadRedo(lsn uint64) ([]byte, bool) {
	s.mu.RLock()
	pos, ok := s.redoMap[lsn]
	f := s.redoF
	if !ok || f == nil {
		s.mu.RUnlock()
		return nil, false
	}
	s.redoIoMu.RLock()
	s.mu.RUnlock()
	defer s.redoIoMu.RUnlock()
	payload, err := s.readRedoAt(f, pos)
	if err != nil {
		return nil, false
	}
	return payload, true
}

// ReadRedoRaw 返回某个 lsn 的原始成帧记录字节(u32 长度前缀 + framer 体), 即
// 业务读方要往下游发的形态; O_DIRECT 时走对齐缓冲读并裁回真实长度。
// ReadRedoRaw returns the raw framed record bytes (u32 length prefix +
// framer body) for an lsn — what a business reader ships downstream.
func (s *Store) ReadRedoRaw(lsn uint64) ([]byte, bool) {
	s.mu.RLock()
	pos, ok := s.redoMap[lsn]
	f := s.redoF
	direct := s.directIO
	if !ok || f == nil {
		s.mu.RUnlock()
		return nil, false
	}
	s.redoIoMu.RLock()
	s.mu.RUnlock()
	defer s.redoIoMu.RUnlock()
	if direct {
		raw, err := readAtAligned(f, pos.off, pos.length)
		if err != nil {
			return nil, false
		}
		return raw[:pos.length], true
	}
	raw := make([]byte, pos.length)
	if _, err := f.ReadAt(raw, pos.off); err != nil {
		return nil, false
	}
	return raw, true
}

// ---------------------------------------------------------------------------
// 12.9 helpers (best-effort, probed with fallback).

// fdatasync 对 fd 做 Fdatasync(不刷 inode mtime); 失败静默回退到 fsync。
func fdatasync(f *os.File) error {
	if err := unix.Fdatasync(int(f.Fd())); err != nil {
		return f.Sync()
	}
	return nil
}

// preallocate 用 fallocate 把下一个段(默认 64MiB)预分配到目标边界, 避免运行中
// 才做块分配; off 已落在段起点则 no-op。
func preallocate(f *os.File, off, segSize int64) error {
	sz := segSize
	if sz <= 0 {
		sz = 64 << 20
	}
	target := ((off / sz) + 1) * sz
	n := target - off
	if n <= 0 {
		return nil
	}
	return unix.Fallocate(int(f.Fd()), 0, off, n)
}

// syncFileRange 发起异步回写(SYNC_FILE_RANGE_WRITE), 只把脏页推给块层, 不做
// 屏障 —— 真正的持久化仍由 barrier 负责。
func syncFileRange(f *os.File, off, n int64) error {
	return unix.SyncFileRange(int(f.Fd()), off, n, unix.SYNC_FILE_RANGE_WRITE)
}

// fadviseDontNeed 提示内核 [off, off+n) 的页可以丢弃(截断尾巴后清 page cache)。
func fadviseDontNeed(f *os.File, off, n int64) error {
	return unix.Fadvise(int(f.Fd()), off, n, unix.FADV_DONTNEED)
}
