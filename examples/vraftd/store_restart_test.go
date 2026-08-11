// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

// Regression test for the log-store restart overwrite (fixed 2026-08):
//
// reload() replays the log file with ReadAt, which never moves the *os.File
// cursor, so after opening an existing file the cursor was left at 0 while
// endOff pointed at EOF. The first StoreLogs after restart then wrote at
// offset 0, silently overwriting the head of the log with the new leader's
// noop + command entries. In-memory state stayed healthy (reload had loaded
// the full log), so the corruption only surfaced on the *next* restart, when
// reload hit the framing discontinuity and NewRaft panicked with
// "log not found" scanning the config entries from index 1.
//
// This replays the exact sequence — write, restart, append, restart — and
// requires every record to survive in order.
func TestFileLogStoreRestartNoOverwrite(t *testing.T) {
	dir := t.TempDir()
	newStore := func() *fileLogStore {
		s, err := newFileLogStore(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return s
	}

	store := newStore()
	for i := uint64(1); i <= 50; i++ {
		l := &raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: []byte{byte(i)}}
		if err := store.StoreLog(l); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	store.Close()

	// Restart #1: reload then append two more records (the noop + command that
	// every leader emits after taking over).
	store = newStore()
	for i := uint64(51); i <= 52; i++ {
		l := &raft.Log{Index: i, Term: 2, Type: raft.LogCommand, Data: []byte{byte(i)}}
		if err := store.StoreLog(l); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	store.Close()

	// Restart #2: the whole history must reload cleanly and in order.
	store = newStore()
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 1 || last != 52 {
		t.Fatalf("bad range after restarts: first=%d last=%d (want 1..52)", first, last)
	}
	for i := uint64(1); i <= 52; i++ {
		var l raft.Log
		if err := store.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) after restart: %v", i, err)
		}
		if l.Index != i {
			t.Fatalf("record %d read back as index %d", i, l.Index)
		}
	}
	if fi, err := os.Stat(dir + "/raft.log"); err != nil || fi.Size() == 0 {
		t.Fatalf("log file missing or empty after restart: %v", err)
	}
	store.Close()
}

// The stable store must persist current term and voted-for across a restart,
// otherwise a restarted node forgets its term and may start spurious elections.
func TestFileStableStorePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s1, err := newFileStableStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s1.SetUint64([]byte("CurrentTerm"), 7); err != nil {
		t.Fatalf("set term: %v", err)
	}
	if err := s1.Set([]byte("VotedFor"), []byte("node-2")); err != nil {
		t.Fatalf("set vote: %v", err)
	}

	s2, err := newFileStableStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	term, _ := s2.GetUint64([]byte("CurrentTerm"))
	if term != 7 {
		t.Fatalf("term after restart = %d, want 7", term)
	}
	vote, _ := s2.Get([]byte("VotedFor"))
	if string(vote) != "node-2" {
		t.Fatalf("votedFor after restart = %q, want node-2", vote)
	}
}
