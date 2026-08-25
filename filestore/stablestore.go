// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
)

// StableStore persists raft's current term / voted-for as a small JSON file
// (write tmp + rename, same durability pattern as before).
type StableStore struct {
	mu   sync.Mutex
	path string
	Term uint64 `json:"current_term"`
	Vote []byte `json:"voted_for"`
}

var _ raft.StableStore = (*StableStore)(nil)

// OpenStable opens (or creates) the stable store in dir.
func OpenStable(dir string) (*StableStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "stable.json")
	s := &StableStore{path: path}
	raw, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(raw, s)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

func (s *StableStore) persist() error {
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

func (s *StableStore) Set(key []byte, val []byte) error {
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

func (s *StableStore) Get(key []byte) ([]byte, error) {
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

func (s *StableStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, val)
	return s.Set(key, buf)
}

func (s *StableStore) GetUint64(key []byte) (uint64, error) {
	buf, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(buf), nil
}
