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

// OpenStable 在 dir 下打开(或创建)stable.json 作为 raft 的 StableStore。
// 文件不存在视为空库(term=0、无投票); 其它读取错误原样上抛。
//
// OpenStable opens (or creates) the stable store in dir.
func OpenStable(dir string) (*StableStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "stable.json")
	s := &StableStore{path: path}
	raw, err := os.ReadFile(path)
	if err == nil {
		// 注意: 解析失败被静默吞掉 —— 损坏的 stable.json 会静默退化为
		// term=0 / 无投票, 而不是报错。此处保持既有行为, 未改。
		_ = json.Unmarshal(raw, s)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

// persist 把当前 Term/Vote 序列化后以「写 .tmp + rename」原子替换 stable.json,
// 保证读者永远看不到半个文件。注意只保证 rename 的原子性, 未对目录做 fsync ——
// 掉电可能丢掉最近一次更新(raft 允许: 任期/投票回退由选举协议自身的持久性约束兜底)。
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

// Set 写入 raft 的两个键: CurrentTerm(取 val 前 8B 小端)与 VotedFor(拷贝副本)。
// 未知 key 静默忽略(仍返回 nil); 每次写都落盘(persist)。
func (s *StableStore) Set(key []byte, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch string(key) {
	case "CurrentTerm":
		// 不足 8B 的 val 被静默忽略(不报错、不落盘 Term) —— 既有行为, 未改。
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

// Get 读回 CurrentTerm(8B 小端)或 VotedFor 的副本; 未知 key 返回 (nil, nil)。
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

// SetUint64 把 val 编成 8B 小端后转交 Set(键名语义与 Set 一致)。
func (s *StableStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, val)
	return s.Set(key, buf)
}

// GetUint64 走 Get 读回 8B 小端值; 键不存在(空结果)返回 0 而非错误。
func (s *StableStore) GetUint64(key []byte) (uint64, error) {
	buf, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	// 空 = 键不存在或值为空, 统一按 0 处理(与 raft 的「未设置」语义一致)。
	if len(buf) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(buf), nil
}
