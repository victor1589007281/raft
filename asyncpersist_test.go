// Copyright IBM Corp. 2025, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// delayedLogStore wraps a LogStore and artificially delays StoreLogs to model
// an fsync-heavy durable store, letting tests observe how persistence latency
// interacts with replication.
type delayedLogStore struct {
	LogStore
	delay time.Duration
}

func (d *delayedLogStore) StoreLogs(logs []*Log) error {
	// Only model durable-write latency for real entries; empty heartbeats must
	// not be delayed or the leader would lose contact with quorum.
	if d.delay > 0 && len(logs) > 0 {
		time.Sleep(d.delay)
	}
	return d.LogStore.StoreLogs(logs)
}

// makeAsyncCluster builds a 3-node cluster whose raft log stores are wrapped
// in delayedLogStore. Node 0 uses leaderDelay and the others followerDelay;
// nodes 1 and 2 get a longer election timeout so node 0 reliably becomes
// leader (whose durable store is the one under test).
func makeAsyncCluster(t *testing.T, conf *Config, leaderDelay, followerDelay time.Duration) *cluster {
	c := &cluster{
		observationCh:    make(chan Observation, 1024),
		conf:             conf,
		propagateTimeout: conf.HeartbeatTimeout*2 + conf.CommitTimeout,
		longstopTimeout:  20 * time.Second,
		logger:           newTestLoggerWithPrefix(t, "cluster"),
		failedCh:         make(chan struct{}),
	}
	c.t = t

	var configuration Configuration
	logStores := make([]LogStore, 0, 3)
	for i := 0; i < 3; i++ {
		dir, _ := os.MkdirTemp("", "raft")
		store := NewInmemStore()
		delay := followerDelay
		if i == 0 {
			delay = leaderDelay
		}
		dstore := &delayedLogStore{LogStore: store, delay: delay}
		logStores = append(logStores, dstore)
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &MockFSM{})

		dir2, snap := FileSnapTest(t)
		c.dirs = append(c.dirs, dir2)
		c.snaps = append(c.snaps, snap)

		// Use a generous RPC timeout: the delayed stores simulate fsyncs far
		// slower than the default 500ms transport deadline, and a slow follower
		// must still be allowed to ack.
		addr, trans := NewInmemTransportWithTimeout("", 10*time.Second)
		c.trans = append(c.trans, trans)
		localID := ServerID(fmt.Sprintf("server-%s", addr))
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       localID,
			Address:  addr,
		})
	}

	c.FullyConnect()
	c.startTime = time.Now()

	for i := 0; i < 3; i++ {
		peerConf := conf
		// Bias node 0 to win the first election: give followers a longer
		// election timeout so the delayed-store node becomes leader.
		if i != 0 {
			peerConf = conf
			peerConf.ElectionTimeout = 2 * conf.ElectionTimeout
			peerConf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
		}
		peerConf.LocalID = configuration.Servers[i].ID
		peerConf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[i].ID))

		if err := BootstrapCluster(peerConf, logStores[i], c.stores[i], c.snaps[i], c.trans[i], configuration); err != nil {
			t.Fatalf("BootstrapCluster failed: %v", err)
		}
		raft, err := NewRaft(peerConf, c.fsms[i], logStores[i], c.stores[i], c.snaps[i], c.trans[i])
		if err != nil {
			t.Fatalf("NewRaft failed: %v", err)
		}
		raft.RegisterObserver(NewObserver(c.observationCh, false, nil))
		c.rafts = append(c.rafts, raft)
	}
	return c
}

// asyncTestConf returns a config whose timeouts accommodate simulated fsync
// delays of hundreds of ms. The default 50ms leader-lease would make a leader
// step down before a slow follower can ack the noop, so the lease is raised to
// cover the follower's slow store write while CommitTimeout keeps replication
// contacts frequent enough to stay within the lease.
func asyncTestConf(t *testing.T, async bool) *Config {
	conf := inmemConfig(t)
	conf.AsyncLeaderPersist = async
	conf.HeartbeatTimeout = 1 * time.Second
	conf.ElectionTimeout = 1 * time.Second
	conf.LeaderLeaseTimeout = 800 * time.Millisecond
	conf.CommitTimeout = 100 * time.Millisecond
	return conf
}

// waitApply resolves the future with a timeout.
func waitApply(t *testing.T, f ApplyFuture, desc string) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- f.Error() }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("%s: apply failed: %v", desc, err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("%s: apply did not complete", desc)
	}
}

// TestRaft_AsyncPersist_NoCommitBeforeLeaderFsync is the core safety property
// (etcd issue #18): with followers persisting near-instantly, the entry must
// NOT be committed until the leader's own 300ms fsync completes, because a
// quorum of followers alone cannot advance the commit index past the leader's
// durable log. If the leader's match cap is broken this resolves in ms.
func TestRaft_AsyncPersist_NoCommitBeforeLeaderFsync(t *testing.T) {
	conf := asyncTestConf(t, true)
	c := makeAsyncCluster(t, conf, 300*time.Millisecond, 0)
	defer c.Close()

	leader := c.Leader()
	if leader.localID != c.rafts[0].localID {
		t.Fatalf("node 0 did not become leader: %s", leader.localID)
	}

	start := time.Now()
	f := leader.Apply([]byte("safe"), 0)
	waitApply(t, f, "leader fsync gate")

	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("entry committed before the leader's own fsync: elapsed %s", elapsed)
	}
	t.Logf("single apply gated by leader 300ms fsync took %s", elapsed)
	c.EnsureSame(t)
}

// TestRaft_AsyncPersist_OverlapsReplication verifies the throughput win of
// 提前 fsync: the leader's fsync overlaps the followers' fsync, so a batch
// whose leader store takes 300ms and followers take 500ms commits in roughly
// max(300,500)=500ms rather than the serial 300+500=800ms.
func TestRaft_AsyncPersist_OverlapsReplication(t *testing.T) {
	conf := asyncTestConf(t, true)
	c := makeAsyncCluster(t, conf, 300*time.Millisecond, 500*time.Millisecond)
	defer c.Close()

	leader := c.Leader()
	if leader.localID != c.rafts[0].localID {
		t.Fatalf("node 0 did not become leader: %s", leader.localID)
	}
	start := time.Now()
	waitApply(t, leader.Apply([]byte("overlap"), 0), "async")
	asyncElapsed := time.Since(start)

	// Serial baseline: same topology with the upstream synchronous path, where
	// the leader's store write blocks replication (300 + 500 ≈ 800ms).
	conf2 := asyncTestConf(t, false)
	c2 := makeAsyncCluster(t, conf2, 300*time.Millisecond, 500*time.Millisecond)
	defer c2.Close()

	leader2 := c2.Leader()
	if leader2.localID != c2.rafts[0].localID {
		t.Fatalf("node 0 did not become leader: %s", leader2.localID)
	}
	start2 := time.Now()
	waitApply(t, leader2.Apply([]byte("baseline"), 0), "sync")
	syncElapsed := time.Since(start2)

	t.Logf("async persist=%s vs sync persist=%s", asyncElapsed, syncElapsed)
	if asyncElapsed >= syncElapsed*95/100 {
		t.Fatalf("expected async persist to overlap replication: async=%s >= sync=%s",
			asyncElapsed, syncElapsed)
	}
}

// TestRaft_AsyncPersist_PipelinedBatchesInOrder verifies the async writer
// serializes overlapping batches (so the store and last-log index advance
// monotonically) and that all entries commit in order.
func TestRaft_AsyncPersist_PipelinedBatchesInOrder(t *testing.T) {
	conf := asyncTestConf(t, true)
	c := makeAsyncCluster(t, conf, 200*time.Millisecond, 0)
	defer c.Close()

	leader := c.Leader()
	if leader.localID != c.rafts[0].localID {
		t.Fatalf("node 0 did not become leader: %s", leader.localID)
	}

	const n = 3
	start := time.Now()
	futures := make([]ApplyFuture, n)
	// Apply with gaps shorter than the leader fsync so the batches overlap in
	// the pending buffer (each Apply's unbuffered applyCh dispatch yields a
	// separate batch while the previous batch is still fsyncing).
	for i := 0; i < n; i++ {
		futures[i] = leader.Apply([]byte{byte('a' + i)}, 0)
		time.Sleep(40 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		waitApply(t, futures[i], fmt.Sprintf("batch %d", i))
	}
	elapsed := time.Since(start)

	// Three 200ms fsyncs serialized end-to-end ≈ 600ms; two batches ≈ 400ms.
	// A concurrent writer would finish all three around one fsync (~200ms).
	if elapsed < 400*time.Millisecond {
		t.Fatalf("expected serialized leader fsyncs for overlapping batches, got %s", elapsed)
	}
	t.Logf("3 overlapping batches serialized in %s", elapsed)
	c.EnsureSame(t)
}
