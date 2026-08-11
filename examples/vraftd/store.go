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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
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
//	[u32 payloadLen] [msgpack(Log)]  [u32 payloadLen] [msgpack(Log)]  ...
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
}

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
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(buf, lenBuf[:]); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		n := int64(int32(binary.LittleEndian.Uint32(lenBuf[:])))
		if n <= 0 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(buf, body); err != nil {
			// Crash-truncated tail; ignore.
			break
		}
		l := &raft.Log{}
		if err := codec.NewDecoderBytes(body, &msgpackHandle).Decode(l); err != nil {
			break
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, off)
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
	for _, l := range logs {
		body, err := encodeLog(l)
		if err != nil {
			return err
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(body)))
		if _, err := s.f.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := s.f.Write(body); err != nil {
			return err
		}
		if s.first == 0 {
			s.first = l.Index
		}
		s.logs = append(s.logs, l)
		s.offsets = append(s.offsets, s.endOff)
		s.endOff += 4 + int64(len(body))
	}
	// Durability barrier: the entries are not durable until this returns.
	return s.f.Sync()
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
	if err := s.f.Truncate(truncAt); err != nil {
		return err
	}
	if _, err := s.f.Seek(truncAt, io.SeekStart); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.endOff = truncAt
	return nil
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
