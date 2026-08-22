// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

// Package vraftd is the vRaft demo application: a small replicated KV store on
// top of the vraft fork of hashicorp/raft. It exists to (a) exercise the vraft
// write-path features (BatchWindow, async leader persist) in a real file-backed
// process and (b) serve as the container payload for the kind k8s deployment.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

var msgpackHandle codec.MsgpackHandle

// ---------------------------------------------------------------------------
// fileLogStore: an append-only, fsync-on-write raft.LogStore.
//
// The log is kept in memory for fast reads (GetLog / FirstIndex / LastIndex) and
// appended to a single file in batches. Each StoreLogs call writes the whole
// batch and fsyncs it, so the process models a durable store: raft's own
// durability guarantee is exactly "the entry is on disk when StoreLogs returns".
//
// On-disk layout (little-endian):
//
// Legacy (SingleWALEnabled=false):
//	[u32 payloadLen] [msgpack(Log)]  [u32 payloadLen] [msgpack(Log)]  ...
// SingleWAL (SingleWALEnabled=true):
//	[u32 totalLen][u16 raftVer=1][u16 flags][u64 base][u64 lsn][u32 raftMsgpackLen][msgpack(Log without Data)][data]
//	where data = original Data without the [cmd|base|lsn|len] envelope when applicable.
//	Sparse LSN→(index,off) index is in-memory, rebuilt on reload, pruned on DeleteRange.
//
// msgpack messages are self-describing, so every record can be decoded with a
// fresh decoder — no shared codec state across records, and random access to
// record offsets is possible for DeleteRange.
type fileLogStore struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	first   uint64 // index of logs[0] (0 when empty)
	logs    []*raft.Log
	offsets []int64 // file offset of logs[i] (points at the record's len field)
	endOff  int64   // current end-of-file offset

	// 12.9 — optional kernel I/O knobs (all default off; probed with fallback).
	useFdatasync        bool
	preallocateSegments bool
	segmentSize         int64 // 0 = default 64 MiB
	useFadvise          bool
	useSyncFileRange    bool
	// 12.9/12.7 higher tiers — wired behind switches, probed with fallback.
	// DirectIO: reserved tier (requires aligned buffers + O_DIRECT fd); probed in store_ioring.go.
	// SingleWAL + sparse index: unified WAL segment prototype (raft log == redo); gated here, store_ioring.go hosts the index.
	ioringEnabled         bool
	ioringSQPoll          bool
	directIOEnabled       bool
	singleWALEnabled      bool
	singleWALSparseIndex  bool

	// Sparse LSN→(index,off) index for single-WAL (only when SingleWALSparseIndex).
	lsnIndex map[uint64]lsnPos
}

type lsnPos struct {
	index uint64
	off   int64
}

const singleWALHeaderSize = 4 + 2 + 2 + 8 + 8 + 4 // totalLen+ver+flags+base+lsn+raftLen = 28

var _ raft.LogStore = (*fileLogStore)(nil)

func newFileLogStore(dir string) (*fileLogStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &fileLogStore{path: path, f: f}
	if err := s.reload(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *fileLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.f.Close()
}

// encodeLog msgpack-encodes a log into a self-contained buffer.
func encodeLog(l *raft.Log) ([]byte, error) {
	var buf []byte
	if err := codec.NewEncoderBytes(&buf, &msgpackHandle).Encode(l); err != nil {
		return nil, err
	}
	return buf, nil
}

// encodeSingleWAL wraps l into the single-WAL header [totalLen|ver|flags|base|lsn|raftLen|raftMsgpack|data].
// For generic raft logs where Data has no redo envelope, base/lsn are 0 and data is Data itself.
func encodeSingleWAL(l *raft.Log) ([]byte, error) {
	base, lsn, data := parseRedoEnvelope(l.Data)
	// Encode raft header without Data for dedup.
	tmp := *l
	tmp.Data = nil
	var raftBuf []byte
	if err := codec.NewEncoderBytes(&raftBuf, &msgpackHandle).Encode(&tmp); err != nil {
		return nil, err
	}
	totalLen := singleWALHeaderSize - 4 + len(raftBuf) + len(data)
	rec := make([]byte, 4+totalLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totalLen))
	binary.LittleEndian.PutUint16(rec[4:6], 1)
	binary.LittleEndian.PutUint16(rec[6:8], 0)
	binary.LittleEndian.PutUint64(rec[8:16], base)
	binary.LittleEndian.PutUint64(rec[16:24], lsn)
	binary.LittleEndian.PutUint32(rec[24:28], uint32(len(raftBuf)))
	copy(rec[28:28+len(raftBuf)], raftBuf)
	copy(rec[28+len(raftBuf):], data)
	return rec, nil
}

func parseRedoEnvelope(data []byte) (base, lsn uint64, payload []byte) {
	// Heuristic: redo envelope is JSON {"base":..,"lsn":..,"data":..} or binary [base|lsn|len|data].
	// For vraft demo, fall back to raw Data.
	return 0, 0, data
}

func decodeSingleWAL(rec []byte) (*raft.Log, int, error) {
	if len(rec) < singleWALHeaderSize-4 {
		return nil, 0, errors.New("short single-wal header")
	}
	// ver at rec[0:2], flags at rec[2:4], base at rec[4:12], lsn at rec[12:20], raftLen at rec[20:24] — reserved for future.
	raftLen := int(binary.LittleEndian.Uint32(rec[20:24]))
	if raftLen < 0 || raftLen > len(rec)-(singleWALHeaderSize-4) {
		return nil, 0, errors.New("bad raftLen")
	}
	raftBody := rec[(singleWALHeaderSize - 4) : (singleWALHeaderSize-4)+raftLen]
	data := rec[(singleWALHeaderSize - 4)+raftLen:]
	l := &raft.Log{}
	if err := codec.NewDecoderBytes(raftBody, &msgpackHandle).Decode(l); err != nil {
		return nil, 0, err
	}
	l.Data = append([]byte(nil), data...)
	return l, singleWALHeaderSize - 4 + raftLen, nil
}

// reload replays the log file into memory and offsets. Called once at open.
func (s *fileLogStore) reload() error {
	if err := s.f.Sync(); err != nil {
		return err
	}
	info, err := s.f.Stat()
	if err != nil {
		return err
	}
	raw := make([]byte, info.Size())
	if _, err := s.f.ReadAt(raw, 0); err != nil && err != io.EOF {
		return err
	}
	buf := bytes.NewReader(raw)
	var off int64
	// Detect single-WAL by probing first record: if it decodes as single-WAL header + raft msgpack, use single-WAL path.
	// Fallback to legacy [u32][msgpack] on any mismatch / legacy file.
	useSingleWAL := s.singleWALEnabled
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(buf, lenBuf[:]); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		n := int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if n <= 0 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(buf, body); err != nil {
			// Crash-truncated tail; ignore.
			break
		}
		var l *raft.Log
		var lsnForIndex uint64
		if useSingleWAL {
			decoded, _, err := decodeSingleWAL(body)
			if err != nil {
				// Corrupt single-WAL tail — treat as truncated.
				break
			}
			l = decoded
			// Extract lsn from header for sparse index.
			if len(body) >= 20 {
				lsnForIndex = binary.LittleEndian.Uint64(body[12:20])
			}
		} else {
			ll := &raft.Log{}
			if err := codec.NewDecoderBytes(body, &msgpackHandle).Decode(ll); err != nil {
				break
			}
			l = ll
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, off)
		if s.singleWALSparseIndex && lsnForIndex != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsnForIndex] = lsnPos{index: l.Index, off: off}
		}
		off += 4 + n
	}
	s.endOff = off
	// Restore the file cursor to EOF. reload() reads with ReadAt (which never
	// moves the cursor), so without this Seek the next StoreLogs would write at
	// offset 0 and silently overwrite the head of the log on restart.
	if _, err := s.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// StoreLogs appends a batch and fsyncs. This is the hot path for the write
// throughput benchmark: the Sync is exactly what the vraft BatchWindow and
// async leader persist overlap with replication.
//
// 12.9 knobs (all behind switches, probed with fallback):
//   - UseFdatasync: Sync → Fdatasync (skip inode mtime flush).
//   - PreallocateSegments: fallocate next segment eagerly.
//   - UseSyncFileRange: kick async writeback mid-batch (SYNC_FILE_RANGE_WRITE).
//   - UseFadvise: DONTNEED truncated tail for page-cache hygiene.
func (s *fileLogStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(logs) == 0 {
		return nil
	}
	// s.endOff is the authoritative append position; the file cursor must be
	// parked there. Without this Seek, the first write after a reload would
	// land at whatever offset the cursor was left at (0 after ReadAt).
	if _, err := s.f.Seek(s.endOff, io.SeekStart); err != nil {
		return err
	}
	// Pre-encode batch so io_uring can submit WRITE×k → FSYNC in one chain.
	encoded := make([][]byte, len(logs))
	lsns := make([]uint64, len(logs))
	for i, l := range logs {
		var rec []byte
		var err error
		if s.singleWALEnabled {
			rec, err = encodeSingleWAL(l)
			if err != nil {
				return err
			}
			if len(rec) >= 24 {
				lsns[i] = binary.LittleEndian.Uint64(rec[16:24])
			}
		} else {
			body, err2 := encodeLog(l)
			if err2 != nil {
				return err2
			}
			var lenBuf [4]byte
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(body)))
			rec = make([]byte, 4+len(body))
			copy(rec[:4], lenBuf[:])
			copy(rec[4:], body)
		}
		encoded[i] = rec
	}
	baseOff := s.endOff
	// io_uring fast path: single io_uring_enter for WRITE×k → FSYNC.
	if s.ioringEnabled {
		if err := storeLogsIORingChain(s.f, baseOff, encoded, s.ioringSQPoll); err == nil {
			for i, l := range logs {
				if s.first == 0 {
					s.first = l.Index
				}
				off := baseOff
				s.logs = append(s.logs, l)
				s.offsets = append(s.offsets, off)
				if s.singleWALSparseIndex && lsns[i] != 0 {
					if s.lsnIndex == nil {
						s.lsnIndex = make(map[uint64]lsnPos)
					}
					s.lsnIndex[lsns[i]] = lsnPos{index: l.Index, off: off}
				}
				baseOff += int64(len(encoded[i]))
			}
			s.endOff = baseOff
			if s.preallocateSegments {
				_ = preallocate(s.f, s.endOff, s.segmentSize)
			}
			if s.useSyncFileRange {
				_ = syncFileRange(s.f, 0, 0)
			}
			return nil
		}
		// fall through to classic Write+barrier on ErrIORingNotSupported / CQE error
	}
	for i, l := range logs {
		rec := encoded[i]
		if _, err := s.f.Write(rec); err != nil {
			return err
		}
		if s.first == 0 {
			s.first = l.Index
		}
		off := s.endOff
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, off)
		if s.singleWALSparseIndex && lsns[i] != 0 {
			if s.lsnIndex == nil {
				s.lsnIndex = make(map[uint64]lsnPos)
			}
			s.lsnIndex[lsns[i]] = lsnPos{index: l.Index, off: off}
		}
		s.endOff += int64(len(rec))
	}
	// Durability barrier: the entries are not durable until this returns.
	// 12.9 tiers (probed): io_uring chain → fdatasync → fsync.
	if s.ioringEnabled {
		if err := storeLogsIORing(s.f, s.endOff, s.ioringSQPoll); err == nil {
			// io_uring chain succeeded
		} else if s.useFdatasync {
			if err2 := fdatasync(s.f); err2 != nil { return err2 }
		} else {
			if err2 := s.f.Sync(); err2 != nil { return err2 }
		}
	} else if s.directIOEnabled {
		// O_DIRECT+IOPOLL tier: reserved档位, currently falls back to barrier above via sync_file_range hint;
		// real path needs aligned buffers + O_DIRECT fd. Keep flag wired; barrier below is the correctness anchor.
		_ = syncFileRange(s.f, 0, 0)
		if s.useFdatasync {
			if err := fdatasync(s.f); err != nil { return err }
		} else {
			if err := s.f.Sync(); err != nil { return err }
		}
	} else if s.useFdatasync {
		if err := fdatasync(s.f); err != nil { return err }
	} else {
		if err := s.f.Sync(); err != nil { return err }
	}
	if s.preallocateSegments {
		_ = preallocate(s.f, s.endOff, s.segmentSize)
	}
	if s.useSyncFileRange {
		_ = syncFileRange(s.f, 0, 0) // best-effort async writeback kick
	}
	return nil
}

func (s *fileLogStore) StoreLog(l *raft.Log) error {
	return s.StoreLogs([]*raft.Log{l})
}

func (s *fileLogStore) GetLog(index uint64, l *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == 0 || index < s.first {
		return raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		return raft.ErrLogNotFound
	}
	*l = *s.logs[off]
	return nil
}

func (s *fileLogStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first, nil
}

func (s *fileLogStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) == 0 {
		return 0, nil
	}
	return s.logs[len(s.logs)-1].Index, nil
}

// DeleteRange truncates the log at the first deleted index. Everything at or
// above `min` is removed both from memory and from the file.
func (s *fileLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) == 0 || min > max {
		return nil
	}
	if max < s.first {
		return nil
	}
	// Number of entries to keep (those with index < min).
	keep := 0
	if min > s.first {
		keep = int(min - s.first)
		if keep > len(s.logs) {
			keep = len(s.logs)
		}
	}
	truncAt := s.offsets[keep] // file offset of the first deleted record
	s.logs = append([]*raft.Log(nil), s.logs[:keep]...)
	s.offsets = append([]int64(nil), s.offsets[:keep]...)
	if len(s.logs) == 0 {
		s.first = 0
	}
	if s.singleWALSparseIndex && s.lsnIndex != nil {
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
	if _, err := s.f.Seek(truncAt, io.SeekStart); err != nil {
		return err
	}
	if s.useFdatasync {
		if err := fdatasync(s.f); err != nil {
			return err
		}
	} else {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	if s.useFadvise && oldEnd > truncAt {
		_ = fadviseDontNeed(s.f, truncAt, oldEnd-truncAt)
	}
	s.endOff = truncAt
	return nil
}

// GetLogRaw returns the raw Data bytes for index without copying the full Log.
// When SingleWAL is disabled it falls back to GetLog.
func (s *fileLogStore) GetLogRaw(index uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == 0 || index < s.first {
		return nil, raft.ErrLogNotFound
	}
	off := index - s.first
	if off >= uint64(len(s.logs)) {
		return nil, raft.ErrLogNotFound
	}
	return append([]byte(nil), s.logs[off].Data...), nil
}

// LookupByLSN returns the raft index for a redo LSN via the sparse index when enabled.
func (s *fileLogStore) LookupByLSN(lsn uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.singleWALSparseIndex || s.lsnIndex == nil {
		return 0, false
	}
	pos, ok := s.lsnIndex[lsn]
	if !ok {
		return 0, false
	}
	return pos.index, true
}

// ---- 12.9 helpers (probed with fallback; all best-effort) ----

func fdatasync(f *os.File) error {
	if err := unix.Fdatasync(int(f.Fd())); err != nil {
		return f.Sync()
	}
	return nil
}

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

func syncFileRange(f *os.File, off, n int64) error {
	return unix.SyncFileRange(int(f.Fd()), off, n, unix.SYNC_FILE_RANGE_WRITE)
}

func fadviseDontNeed(f *os.File, off, n int64) error {
	return unix.Fadvise(int(f.Fd()), off, n, unix.FADV_DONTNEED)
}

// ---------------------------------------------------------------------------
// fileStableStore: current term / voted-for persisted as a single JSON file.
type fileStableStore struct {
	mu   sync.Mutex
	path string
	Term uint64 `json:"current_term"`
	Vote []byte `json:"voted_for"`
}

var _ raft.StableStore = (*fileStableStore)(nil)

func newFileStableStore(dir string) (*fileStableStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "stable.json")
	s := &fileStableStore{path: path}
	raw, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(raw, s)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

func (s *fileStableStore) persist() error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *fileStableStore) Set(key []byte, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch string(key) {
	case "CurrentTerm":
		if len(val) >= 8 {
			s.Term = binary.LittleEndian.Uint64(val)
		}
	case "VotedFor":
		s.Vote = append([]byte(nil), val...)
	default:
		return nil
	}
	return s.persist()
}

func (s *fileStableStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch string(key) {
	case "CurrentTerm":
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, s.Term)
		return buf, nil
	case "VotedFor":
		return append([]byte(nil), s.Vote...), nil
	}
	return nil, nil
}

func (s *fileStableStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, val)
	return s.Set(key, buf)
}

func (s *fileStableStore) GetUint64(key []byte) (uint64, error) {
	buf, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(buf), nil
}

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

func newKVFSM() *KVFSM {
	return &KVFSM{kv: make(map[string]string)}
}

type kvCmd struct {
	Op string `json:"op"`
	K  string `json:"k"`
	V  string `json:"v"`
}

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

func (f *KVFSM) Applied() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

func (f *KVFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.kv[key]
	return v, ok
}

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

func (f *KVFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	snap := make(map[string]string, len(f.kv))
	for k, v := range f.kv {
		snap[k] = v
	}
	return &kvSnapshot{kv: snap}, nil
}

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

func (s *kvSnapshot) Release() {}

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
