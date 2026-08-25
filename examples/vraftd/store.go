// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

// Package vraftd is the vRaft demo application: a small replicated KV store on
// top of the vraft fork of hashicorp/raft. It exists to (a) exercise the vraft
// write-path features (BatchWindow, async leader persist, single WAL) in a real
// file-backed process and (b) serve as the container payload for the kind k8s
// deployment.
//
// The durable stores live in the library-level package
// github.com/hashicorp/raft/filestore (12.7/12.9/12.14 capabilities: single
// WAL v2 format, CRC torn-tail detection, sparse LSN index, io_uring chain).
// This file keeps only the demo FSM.
package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

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
