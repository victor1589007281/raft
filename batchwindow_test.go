// Copyright IBM Corp. 2025, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sync"
	"testing"
	"time"
)

// batchingCountFSM counts how many times ApplyBatch is invoked and the total
// number of entries applied, so tests can assert on coalescing behavior.
type batchingCountFSM struct {
	MockFSM
	mu       sync.Mutex
	calls    int
	applied  int
	maxBatch int
	commands int
}

func (b *batchingCountFSM) ApplyBatch(logs []*Log) []interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.applied += len(logs)
	if len(logs) > b.maxBatch {
		b.maxBatch = len(logs)
	}
	for _, l := range logs {
		b.logs = append(b.logs, l.Data)
		if l.Type == LogCommand {
			b.commands++
		}
	}
	resp := make([]interface{}, len(logs))
	for i := range logs {
		resp[i] = len(b.logs) - len(logs) + i + 1
	}
	return resp
}

// commandsApplied counts only LogCommand entries, excluding the configuration
// entries the cluster itself appends during leader election.
func (b *batchingCountFSM) counts() (calls, applied, maxBatch, commands int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls, b.applied, b.maxBatch, b.commands
}

// TestRaft_BatchWindow_Coalesces verifies that a non-zero BatchWindow causes
// concurrent appends landing within the window to be dispatched as a few
// coalesced batches instead of one batch per entry. A 1-node cluster commits
// immediately, so the number of ApplyBatch invocations directly reflects
// applyCh-level batching, which is exactly what BatchWindow controls.
func TestRaft_BatchWindow_Coalesces(t *testing.T) {
	conf := inmemConfig(t)
	conf.BatchWindow = 30 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond

	fsm := &batchingCountFSM{}
	c, err := makeCluster(t, &MakeClusterOpts{
		Peers:        1,
		Bootstrap:    true,
		Conf:         conf,
		MakeFSMFunc:  func() FSM { return fsm },
		LongstopTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to make cluster: %v", err)
	}
	defer c.Close()

	leader := c.Leader()
	const n = 100
	var group sync.WaitGroup
	group.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer group.Done()
			f := leader.Apply([]byte{byte(i)}, 0)
			if err := f.Error(); err != nil {
				t.Errorf("apply %d failed: %v", i, err)
			}
		}(i)
	}

	doneCh := make(chan struct{})
	go func() { group.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("timeout waiting for applies")
	}

	calls, applied, maxBatch, commands := fsm.counts()
	if commands != n {
		t.Fatalf("expected %d commands applied, got %d", n, commands)
	}
	if applied < n {
		t.Fatalf("expected at least %d entries applied, got %d", n, applied)
	}
	// With a 30ms window and all 100 appends queued within the window, the
	// entries must coalesce into far fewer than 100 batches. 64-entry batches
	// would give 2; leave slack for scheduling jitter.
	if calls >= 10 {
		t.Fatalf("expected heavy coalescing (calls << %d), got %d calls (max batch %d)",
			n, calls, maxBatch)
	}
	if maxBatch < 10 {
		t.Fatalf("expected large batches, got max batch %d across %d calls", maxBatch, calls)
	}
}

// TestRaft_BatchWindow_SingleAppendBoundedLatency verifies a lone apply does
// not wait forever: a non-zero window delays dispatch by at most the window
// (plus commit), so the future must resolve within a bounded time.
func TestRaft_BatchWindow_SingleAppendBoundedLatency(t *testing.T) {
	conf := inmemConfig(t)
	conf.BatchWindow = 250 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond

	fsm := &batchingCountFSM{}
	c, err := makeCluster(t, &MakeClusterOpts{
		Peers:        1,
		Bootstrap:    true,
		Conf:         conf,
		MakeFSMFunc:  func() FSM { return fsm },
		LongstopTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to make cluster: %v", err)
	}
	defer c.Close()

	leader := c.Leader()
	start := time.Now()
	f := leader.Apply([]byte("single"), 0)
	errCh := make(chan error, 1)
	go func() { errCh <- f.Error() }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("apply failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("apply did not complete")
	}
	elapsed := time.Since(start)

	// The batch is held open up to the window then dispatched and committed;
	// allow generous slack, but the future must never block indefinitely.
	if elapsed > 3*time.Second {
		t.Fatalf("apply took %s, far beyond window+commit", elapsed)
	}
	if _, _, _, commands := fsm.counts(); commands != 1 {
		t.Fatalf("expected exactly 1 command applied, got %d", commands)
	}
	t.Logf("single apply with BatchWindow=%s resolved in %s", conf.BatchWindow, elapsed)
}
