// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

// 12.14 P4: pointer-tier AppendEntries (redo 条目省略 Data, 指针替代).
//
// With RefReplicationEnabled and a ref-mode log store, the leader MAY elide a
// redo entry's Data when its bytes are (expected to be) already durable on the
// follower via the P3 direct data plane. The pointer rides in Log.Extensions
// (already on the wire):
//
//   Extensions = [0xE1][u32 bodyLen][u64 base][u32 lsnCount][lsns×u64]
//                [u32 dataLen][u32 crc32c(data)]
//
// The follower resolves the marker BEFORE storing: the log store (ref mode)
// reads the payloads from its local redo file by LSN and restores Data in
// memory — the stored entry is complete again, so mixed clusters with stores
// that cannot resolve (legacy/inmem/boltdb) are safe: the follower's raft core
// answers AppendEntriesResponse.DataMissingFrom > 0 and the leader falls back
// to full-data replication for that follower (and sticky-reprobes later).
//
// Correctness notes:
//   - Commit counting is untouched (matchIndex over stored entries); election
//     restriction likewise (term/index live in meta).
//   - Redo semantics make presence idempotent: same LSN ⇒ same bytes.
//   - An elided entry never persists with its marker: resolution either
//     restores Data (store splits + dedup-skips the redo write) or fails the
//     batch before anything is written.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const elideMagicV1 = 0xE1

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// ElideMarker is the decoded pointer payload of an elided redo entry.
type ElideMarker struct {
	Base    uint64
	LSNs    []uint64
	DataLen uint32
	DataCRC uint32
}

// EncodeElideMarker serializes the pointer for Log.Extensions.
func EncodeElideMarker(base uint64, lsns []uint64, dataLen, dataCRC uint32) []byte {
	body := make([]byte, 8+4+8*len(lsns)+8)
	binary.LittleEndian.PutUint64(body[0:8], base)
	binary.LittleEndian.PutUint32(body[8:12], uint32(len(lsns)))
	for i, lsn := range lsns {
		binary.LittleEndian.PutUint64(body[12+8*i:12+8*i+8], lsn)
	}
	binary.LittleEndian.PutUint32(body[len(body)-8:len(body)-4], dataLen)
	binary.LittleEndian.PutUint32(body[len(body)-4:], dataCRC)
	out := make([]byte, 5+len(body))
	out[0] = elideMagicV1
	binary.LittleEndian.PutUint32(out[1:5], uint32(len(body)))
	copy(out[5:], body)
	return out
}

// DecodeElideMarker parses Log.Extensions; returns nil when the extension is
// not an elision marker (or malformed — callers treat that as "not elided").
func DecodeElideMarker(ext []byte) *ElideMarker {
	if len(ext) < 5+24 || ext[0] != elideMagicV1 {
		return nil
	}
	bodyLen := int(binary.LittleEndian.Uint32(ext[1:5]))
	if len(ext) != 5+bodyLen {
		return nil
	}
	body := ext[5:]
	n := int(binary.LittleEndian.Uint32(body[8:12]))
	if len(body) != 8+4+8*n+8 {
		return nil
	}
	m := &ElideMarker{Base: binary.LittleEndian.Uint64(body[0:8]), LSNs: make([]uint64, n)}
	for i := range m.LSNs {
		m.LSNs[i] = binary.LittleEndian.Uint64(body[12+8*i : 12+8*i+8])
	}
	m.DataLen = binary.LittleEndian.Uint32(body[len(body)-8 : len(body)-4])
	m.DataCRC = binary.LittleEndian.Uint32(body[len(body)-4:])
	return m
}

// RedoDataMissingError is returned by a log store that cannot resolve an
// elided entry's bytes from its local redo data.
type RedoDataMissingError struct {
	Index uint64
	LSN   uint64
}

func (e *RedoDataMissingError) Error() string {
	return fmt.Sprintf("redo bytes missing for elided entry index=%d lsn=%d", e.Index, e.LSN)
}

// LogDataElider is implemented by ref-mode log stores (leader side): produce
// a pointer-tier copy of l (Data nil + Extensions marker) when the entry is a
// redo entry worth eliding.
type LogDataElider interface {
	ElideLogData(l *Log) (elided *Log, ok bool)
}

// RedoByteResolver is implemented by ref-mode log stores (follower side):
// restore the Data bytes of an elided entry from the local redo file.
// Must return *RedoDataMissingError when any referenced LSN is absent.
type RedoByteResolver interface {
	ResolveElidedData(extensions []byte) ([]byte, error)
}

// resolveElidedEntries restores Data in-place for entries carrying an elision
// marker. Returns (firstMissingIndex, resolverPresent). Stores without a
// resolver never see Data=nil entries slip through: callers must check
// firstMissingIndex and reject those entries back to the leader.
func resolveElidedEntries(store interface{}, entries []*Log) (uint64, error) {
	for _, e := range entries {
		m := DecodeElideMarker(e.Extensions)
		if m == nil {
			continue
		}
		r, ok := store.(RedoByteResolver)
		if !ok {
			return e.Index, fmt.Errorf("store %T cannot resolve elided redo entry", store)
		}
		data, err := r.ResolveElidedData(e.Extensions)
		if err != nil {
			return e.Index, err
		}
		if uint32(len(data)) != m.DataLen || crc32.Checksum(data, castagnoliTable) != m.DataCRC {
			return e.Index, fmt.Errorf("elided entry %d resolved data mismatch (len %d, want %d)", e.Index, len(data), m.DataLen)
		}
		e.Data = data
		e.Extensions = nil // marker resolved; the stored entry is complete
	}
	return 0, nil
}
