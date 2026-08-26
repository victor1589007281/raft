// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"bytes"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/hashicorp/raft"
)

// The io_uring SQE must be exactly 64 bytes: the kernel reads SQEs at 64-byte
// strides. (Shipped-and-caught: a stray Pad field made sizeof 72; every SQE
// after the first was garbage and the FSYNC degraded into a NOP — plus the
// FSYNC opcode constant was WRITEV(2) instead of FSYNC(3), masked by the size
// bug. Both are guarded here.)
func TestIOUringSQEABI(t *testing.T) {
	if got := unsafe.Sizeof(ioUringSQE{}); got != 64 {
		t.Fatalf("sizeof(ioUringSQE) = %d, want 64 (kernel ABI)", got)
	}
	if got := unsafe.Sizeof(ioUringCQE{}); got != 16 {
		t.Fatalf("sizeof(ioUringCQE) = %d, want 16", got)
	}
	if ioringOpFsync != 3 || ioringOpWrite != 23 {
		t.Fatalf("opcode constants: fsync=%d write=%d", ioringOpFsync, ioringOpWrite)
	}
}

// Regression: the io_uring single-fd chain must actually persist data —
// in-memory GetLog would mask a silently broken chain.
func TestIORingChainLandsOnDisk(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Mode: ModeUnified, Codec: IndexLSNCodec{}, IORing: true}
	s, err := Open(filepath.Join(dir, "logs"), opts)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("Z"), 100)
	if err := s.StoreLog(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: payload}); err != nil {
		t.Fatal(err)
	}
	ok := s.Stats().IORingOK
	t.Logf("ioringOK=%d", ok)
	s.Close()
	s2, err := Open(filepath.Join(dir, "logs"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var l raft.Log
	if err := s2.GetLog(1, &l); err != nil {
		t.Fatalf("chain write did NOT land: %v", err)
	}
	if len(l.Data) != 100 {
		t.Fatalf("landed data len=%d", len(l.Data))
	}
	if ok == 0 {
		t.Log("note: io_uring unavailable on this kernel; classic path exercised")
	}
}

// Regression: the ref-mode dual-fd chain (one enter: WRITE(redo)→FSYNC(redo)
// →WRITE(meta)→FSYNC(meta)) must persist BOTH files.
func TestIORingDualChainLandsBothFiles(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Mode: ModeRef, Codec: IndexLSNCodec{}, IORing: true}
	s, err := Open(filepath.Join(dir, "logs"), opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		if err := s.StoreLog(&raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: bytes.Repeat([]byte{byte('a' + i)}, 64)}); err != nil {
			t.Fatal(err)
		}
	}
	ok := s.Stats().IORingOK
	t.Logf("ioringOK=%d", ok)
	s.Close()
	s2, err := Open(filepath.Join(dir, "logs"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := uint64(1); i <= 4; i++ {
		var l raft.Log
		if err := s2.GetLog(i, &l); err != nil {
			t.Fatalf("dual chain lost entry %d: %v", i, err)
		}
		if len(l.Data) != 64 {
			t.Fatalf("entry %d data len=%d", i, len(l.Data))
		}
		if _, ok := s2.ReadRedo(i); !ok {
			t.Fatalf("dual chain lost redo %d", i)
		}
	}
}
