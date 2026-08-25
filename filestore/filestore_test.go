// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

func mkLogs(start, n uint64, payload string) []*raft.Log {
	logs := make([]*raft.Log, n)
	for i := range logs {
		logs[i] = &raft.Log{
			Index: start + uint64(i),
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte(payload + "-" + string(rune('a'+i))),
		}
	}
	return logs
}

func v2opts() Options {
	return Options{SingleWAL: true, SparseIndex: true, Codec: IndexLSNCodec{}}
}

// TestV2RoundtripRestart: encode→decode roundtrip across a close/reopen,
// sparse index built on write AND rebuilt on reload (LookupByLSN hits).
func TestV2RoundtripRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Format(); got != "v2-singlewal" {
		t.Fatalf("new file format = %q, want v2-singlewal", got)
	}
	logs := mkLogs(1, 8, "val")
	if err := s.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}
	for _, l := range logs {
		if idx, ok := s.LookupByLSN(l.Index); !ok || idx != l.Index {
			t.Fatalf("LookupByLSN(%d) = %d,%v", l.Index, idx, ok)
		}
	}
	// File magic must be on disk (self-describing).
	raw, err := os.ReadFile(filepath.Join(dir, "logs", "raft.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < fileHeaderLen || string(raw[0:4]) != fileMagic {
		t.Fatalf("missing VWAL magic header (len=%d)", len(raw))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen WITHOUT any switch: format must still be detected as v2.
	s2, err := Open(filepath.Join(dir, "logs"), Options{SparseIndex: true, Codec: IndexLSNCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Format(); got != "v2-singlewal" {
		t.Fatalf("reopened format = %q, want v2-singlewal (auto-detect)", got)
	}
	first, _ := s2.FirstIndex()
	last, _ := s2.LastIndex()
	if first != 1 || last != 8 {
		t.Fatalf("first/last = %d/%d, want 1/8", first, last)
	}
	for _, want := range logs {
		var got raft.Log
		if err := s2.GetLog(want.Index, &got); err != nil {
			t.Fatal(err)
		}
		if string(got.Data) != string(want.Data) || got.Term != want.Term || got.Type != want.Type {
			t.Fatalf("roundtrip mismatch idx %d: %q vs %q", want.Index, got.Data, want.Data)
		}
		if idx, ok := s2.LookupByLSN(want.Index); !ok || idx != want.Index {
			t.Fatalf("post-restart LookupByLSN(%d) = %d,%v", want.Index, idx, ok)
		}
	}
	// Stats observability.
	st := s2.Stats()
	if st.LSNEntries != 8 || st.LastIndex != 8 || st.Format != "v2-singlewal" {
		t.Fatalf("stats = %+v", st)
	}
}

// TestV2BatchTailAlignment: every batch lands at a 512-aligned offset and
// endOff stays 512-aligned (O_DIRECT prerequisite, 12.14.3).
func TestV2BatchTailAlignment(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for b := 0; b < 5; b++ {
		if got := s.Stats().EndOffset; got%512 != 0 {
			t.Fatalf("batch %d start off=%d not 512-aligned", b, got)
		}
		if err := s.StoreLogs(mkLogs(uint64(1+b*3), 3, "pad")); err != nil {
			t.Fatal(err)
		}
		if got := s.Stats().EndOffset; got%512 != 0 {
			t.Fatalf("batch %d end off=%d not 512-aligned (batch-tail padding broken)", b, got)
		}
	}
	if s.Stats().LSNEntries != 15 {
		t.Fatalf("lsn entries = %d, want 15", s.Stats().LSNEntries)
	}
}

// TestV2TornTail: a crash mid-write leaves a partial record; reload must stop
// at the last good record, and the next append must overwrite the torn bytes.
func TestV2TornTail(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs(mkLogs(1, 4, "good")); err != nil {
		t.Fatal(err)
	}
	goodEnd := s.Stats().EndOffset
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate crash mid-write: half of a record appended, no barrier.
	f, err := os.OpenFile(filepath.Join(lp, "raft.log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	torn := make([]byte, 37)
	for i := range torn {
		torn[i] = 0xAB
	}
	binary.LittleEndian.PutUint32(torn[0:4], 200) // claims a long body
	if _, err := f.WriteAt(torn, goodEnd); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if last, _ := s2.LastIndex(); last != 4 {
		t.Fatalf("last = %d after torn tail, want 4", last)
	}
	// Append over the torn bytes and verify.
	if err := s2.StoreLogs(mkLogs(5, 2, "after")); err != nil {
		t.Fatal(err)
	}
	if s2.Stats().EndOffset < goodEnd {
		t.Fatalf("endOff regressed: %d < %d", s2.Stats().EndOffset, goodEnd)
	}
	var l raft.Log
	if err := s2.GetLog(6, &l); err != nil || string(l.Data) != "after-b" {
		t.Fatalf("GetLog(6) = %q, %v", l.Data, err)
	}
	if idx, ok := s2.LookupByLSN(6); !ok || idx != 6 {
		t.Fatalf("LookupByLSN(6) = %d,%v", idx, ok)
	}
	// Close + reopen once more: the post-torn appends must survive on disk
	// (the torn garbage beyond endOff must never confuse the scan again).
	s2.Close()
	s3, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if last, _ := s3.LastIndex(); last != 6 {
		t.Fatalf("last = %d after torn+append+restart, want 6", last)
	}
	for i := uint64(1); i <= 6; i++ {
		var gl raft.Log
		if err := s3.GetLog(i, &gl); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
		if _, ok := s3.LookupByLSN(i); !ok {
			t.Fatalf("LookupByLSN(%d) lost after restart", i)
		}
	}
}

// TestV2CRCDetectsCorruption: a flipped byte inside a record must stop the
// scan at that record (CRC32C), not decode garbage.
func TestV2CRCDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs(mkLogs(1, 3, "crc")); err != nil {
		t.Fatal(err)
	}
	secondOff := s.offsets[1] // record 2's offset
	s.Close()

	f, err := os.OpenFile(filepath.Join(lp, "raft.log"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	f.ReadAt(one, secondOff+15) // inside record 2 header
	one[0] ^= 0xFF
	f.WriteAt(one, secondOff+15)
	f.Close()

	s2, err := Open(lp, v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if last, _ := s2.LastIndex(); last != 1 {
		t.Fatalf("last = %d after corruption, want 1 (CRC must reject record 2+)", last)
	}
	if _, ok := s2.LookupByLSN(2); ok {
		t.Fatal("corrupted record must not enter sparse index")
	}
}

// TestLegacyFileStaysLegacy: an existing legacy file keeps receiving legacy
// records even with SingleWAL on — flipping the switch must not corrupt data.
func TestLegacyFileStaysLegacy(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	s, err := Open(lp, Options{}) // legacy
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs(mkLogs(1, 3, "leg")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(lp, v2opts()) // switch flipped ON
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Format(); got != "legacy" {
		t.Fatalf("legacy file opened as %q — must stay legacy", got)
	}
	if err := s2.StoreLogs(mkLogs(4, 2, "leg2")); err != nil {
		t.Fatal(err)
	}
	if got := s2.Format(); got != "legacy" {
		t.Fatalf("format flipped to %q on append", got)
	}
	for i := uint64(1); i <= 5; i++ {
		var l raft.Log
		if err := s2.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
	}
	// No magic header may be written into a legacy file.
	raw, _ := os.ReadFile(filepath.Join(lp, "raft.log"))
	if len(raw) >= 4 && string(raw[0:4]) == fileMagic {
		t.Fatal("legacy file contaminated with VWAL magic")
	}
}

// TestV1PrototypeReadable: files written by the vraftd v1 prototype (no CRC,
// no magic) remain readable.
func TestV1PrototypeReadable(t *testing.T) {
	dir := t.TempDir()
	lp := filepath.Join(dir, "logs")
	if err := os.MkdirAll(lp, 0o755); err != nil {
		t.Fatal(err)
	}
	// Hand-craft a v1 record: [u32 totalLen][u16 ver=1][u16 flags][u64 base][u64 lsn][u32 raftLen][msgpack(Log\Data)][data]
	l := &raft.Log{Index: 7, Term: 3, Type: raft.LogCommand}
	tmp := *l
	tmp.Data = nil
	var raftBuf []byte
	if err := codec.NewEncoderBytes(&raftBuf, &msgpackHandle).Encode(&tmp); err != nil {
		t.Fatal(err)
	}
	payload := []byte("v1-payload")
	totalLen := 24 + len(raftBuf) + len(payload)
	rec := make([]byte, 4+totalLen)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(totalLen))
	binary.LittleEndian.PutUint16(rec[4:6], 1)
	binary.LittleEndian.PutUint64(rec[16:24], 7) // lsn
	binary.LittleEndian.PutUint32(rec[24:28], uint32(len(raftBuf)))
	copy(rec[28:], raftBuf)
	copy(rec[28+len(raftBuf):], payload)
	if err := os.WriteFile(filepath.Join(lp, "raft.log"), rec, 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := Open(lp, Options{SingleWAL: true, SparseIndex: true, Codec: IndexLSNCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Format(); got != "v1-singlewal" {
		t.Fatalf("format = %q, want v1-singlewal", got)
	}
	var got raft.Log
	if err := s.GetLog(7, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "v1-payload" || got.Term != 3 {
		t.Fatalf("v1 roundtrip: %q term=%d", got.Data, got.Term)
	}
	if idx, ok := s.LookupByLSN(7); !ok || idx != 7 {
		t.Fatalf("v1 sparse index: %d,%v", idx, ok)
	}
	// Appends to a v1 file must stay v1 (no format mixing) and survive reopen.
	if err := s.StoreLog(&raft.Log{Index: 8, Term: 3, Type: raft.LogCommand, Data: []byte("v1-appended")}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := Open(lp, Options{SingleWAL: true, SparseIndex: true, Codec: IndexLSNCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var got2 raft.Log
	if err := s2.GetLog(8, &got2); err != nil || string(got2.Data) != "v1-appended" {
		t.Fatalf("v1 append after reopen: %q, %v", got2.Data, err)
	}
	if idx, ok := s2.LookupByLSN(8); !ok || idx != 8 {
		t.Fatalf("v1 appended record index: %d,%v", idx, ok)
	}
}

// TestDeleteRangePrunesAndTruncates (raft contract: drop uncommitted tail).
func TestDeleteRangePrunesAndTruncates(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.StoreLogs(mkLogs(1, 10, "del")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRange(7, 10); err != nil {
		t.Fatal(err)
	}
	if last, _ := s.LastIndex(); last != 6 {
		t.Fatalf("last = %d, want 6", last)
	}
	if _, ok := s.LookupByLSN(8); ok {
		t.Fatal("sparse index not pruned")
	}
	if _, ok := s.LookupByLSN(5); !ok {
		t.Fatal("sparse index over-pruned")
	}
	// Restart: truncation must survive.
	s.Close()
	s2, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if last, _ := s2.LastIndex(); last != 6 {
		t.Fatalf("post-restart last = %d, want 6", last)
	}
}

// TestIORingChainIfAvailable: probe the io_uring chain; where the kernel
// allows it, a v2 batch must round-trip through the single-enter chain.
func TestIORingChainIfAvailable(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), Options{
		SingleWAL: true, SparseIndex: true, Codec: IndexLSNCodec{}, IORing: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.StoreLogs(mkLogs(1, 4, "ior")); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		var l raft.Log
		if err := s.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d): %v (ioring chain may have silently failed)", i, err)
		}
	}
}
