package filestore

// #105-B: GetLog 的**索引空洞**分支必须返回一个 errors.Is(err, raft.ErrLogNotFound) 为真的
// error —— 因为 raft 侧据此决定是否回退到快照(见 replication.go SEND_SNAP)。
//
// 这不是一个"API 风格"问题, 而是一条**自愈路径的通断**:
//   - filestore.GetLog 对 `index < s.first` 与 `off >= len(s.logs)` 返回**裸哨兵**
//     → 身份比较 `err == raft.ErrLogNotFound` 为真 → 走 SEND_SNAP → 快照全覆盖 → 集群自愈;
//   - 但对**中间空洞**(位置算术命中了别的条目, 即 index 落在 [first, first+len) 内却不对应)
//     它返回 `fmt.Errorf("... : %w", raft.ErrLogNotFound)` —— **包装过**。身份比较为假,
//     SEND_SNAP 永不可达, 领导者会在这个索引上每轮重试 fail-loud, 直到永远。
//
// 现场(2026-09-25, logdb-tps 41k 表档): s0 的 raft.log 只剩 18716–18732 连续 17 条,
// 而 commit=18765、lastIdx=18732; 每秒 ~2 行 `failed to get log: index=18714`; 复制永久冻结,
// 调用侧 ApplyRedoBatchBase 在 commit 上无限等待。
//
// 本用例用**真实产者**(真 Store, 真空洞分支, 真 %w)把两种形态一起钉住, 免得日后有人
// 把某一边的返回改成另一种形态而无人察觉。

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

// TestGetLogHoleErrorIsSentinelled 造一个**真**空洞(把 s.first 抬到 logs[0].Index 之上,
// 使 index-first 的位置算术命中别的条目 —— 正是 filestore.go 里那个分支),
// 然后把「裸哨兵」与「空洞」两种返回并列断言。
func TestGetLogHoleErrorIsSentinelled(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "logs"), v2opts())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.StoreLogs(mkLogs(10, 13, "hole")); err != nil {
		t.Fatal(err)
	}

	// (1) 裸哨兵形态: index 在 s.first 之下。
	if err := s.GetLog(9, &raft.Log{}); err == nil {
		t.Fatal("GetLog(9) 应当失败(低于 first)")
	} else {
		if !errors.Is(err, raft.ErrLogNotFound) {
			t.Fatalf("GetLog(9) = %v, errors.Is(ErrLogNotFound) 应为真", err)
		}
		if err != raft.ErrLogNotFound {
			t.Fatalf("GetLog(9) 不再返回裸哨兵(%v) —— 身份比较的调用方会受影响", err)
		}
	}

	// (2) 空洞形态: 把 first 抬高 2, 位置算术 (10-8)=2 → logs[2].Index=12 ≠ 10。
	s.mu.Lock()
	s.first = 8
	s.mu.Unlock()

	err = s.GetLog(10, &raft.Log{})
	if err == nil {
		t.Fatal("GetLog(10) 在空洞下应当失败")
	}
	t.Logf("空洞返回: %v", err)

	if !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("空洞的 error 无法被 errors.Is 认成 ErrLogNotFound —— raft 的快照回退(SEND_SNAP)会被挡在门外: %v", err)
	}
	if err == raft.ErrLogNotFound {
		t.Fatalf("空洞不再包装哨兵 —— 若日后 raft 侧改回身份比较会静默失效; " +
			"请确认 replication.go 仍用 errors.Is 后再改这里")
	}
}
