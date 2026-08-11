// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitForCommitIndex polls until the raft's commit index reaches >= idx or
// the timeout expires.
func waitForCommitIndex(t *testing.T, r *Raft, idx uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.getCommitIndex() >= idx {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for commit index >= %d, current=%d", idx, r.getCommitIndex())
}

// TestVraft_VerifyReadIndex_Leader exercises the three read tiers on the
// leader of a single-node cluster (12.2).
func TestVraft_VerifyReadIndex_Leader(t *testing.T) {
	conf := inmemConfig(t)
	conf.LeaderLeaseTimeout = 40 * time.Millisecond
	c := MakeCluster(1, t, conf)
	defer c.Close()

	leader := c.Leader()
	leaderAddr := leader.localAddr
	c.EnsureLeader(t, leaderAddr)

	// Write one entry so commit/applied indices advance.
	fut := leader.Apply([]byte("write-1"), 0)
	require.NoError(t, fut.Error())
	writeIdx := fut.Index()
	waitForCommitIndex(t, leader, writeIdx, 5*time.Second)

	// ReadStrict: quorum round, returns a watermark >= the write index.
	strictIdx, err := leader.VerifyReadIndex(ReadStrict)
	require.NoError(t, err)
	require.GreaterOrEqual(t, strictIdx, writeIdx)

	// ReadStale: local applied watermark, no leadership requirement.
	staleIdx, err := leader.VerifyReadIndex(ReadStale)
	require.NoError(t, err)
	require.GreaterOrEqual(t, staleIdx, writeIdx)

	// ReadLease: wait for the lease to be established (one lease check on the
	// main thread), then it must return the commit index with no error.
	time.Sleep(100 * time.Millisecond)
	leaseIdx, err := leader.VerifyReadIndex(ReadLease)
	require.NoError(t, err)
	require.GreaterOrEqual(t, leaseIdx, writeIdx)
}

// TestVraft_VerifyReadIndex_Follower checks that strict/lease reads fail with
// ErrNotLeader on a follower while stale reads still work (12.2).
func TestVraft_VerifyReadIndex_Follower(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()
	leaderAddr := leader.localAddr
	c.EnsureLeader(t, leaderAddr)

	fut := leader.Apply([]byte("write-1"), 0)
	require.NoError(t, fut.Error())
	writeIdx := fut.Index()

	// Find a follower.
	var follower *Raft
	for _, r := range c.rafts {
		if r.State() != Leader {
			follower = r
			break
		}
	}
	require.NotNil(t, follower)

	// Strict and lease both require leadership; on a follower they must fail.
	_, err := follower.VerifyReadIndex(ReadStrict)
	require.True(t, errors.Is(err, ErrNotLeader))
	_, err = follower.VerifyReadIndex(ReadLease)
	require.True(t, errors.Is(err, ErrNotLeader))

	// Stale reads work on any node.
	waitForCommitIndex(t, follower, writeIdx, 10*time.Second)
	waitForCommitIndex(t, leader, writeIdx, 10*time.Second)
	staleIdx, err := follower.VerifyReadIndex(ReadStale)
	require.NoError(t, err)
	require.GreaterOrEqual(t, staleIdx, writeIdx)
}

// TestVraft_AddLearnerPromote joins a second node as a learner and promotes it
// to a full voter once it has caught up (12.5.3).
func TestVraft_AddLearnerPromote(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Create a standalone second node. ConfigStoreFSM keeps the joined node's
	// configuration durable across the join, mirroring TestRaft_JoinNode_ConfigStore.
	c1, err := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      false,
		Conf:           conf,
		ConfigStoreFSM: true,
	})
	require.NoError(t, err)
	defer c1.Close()

	c.Merge(c1)
	c.FullyConnect()

	leader := c.Leader()
	leaderAddr := leader.localAddr

	// Add as learner (non-voting member). The config entry commits with the
	// existing voters' quorum, so this succeeds even before the learner's
	// replication catch-up finishes.
	addFut := leader.AddLearner(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	require.NoError(t, addFut.Error())

	// Wait until the learner has learned who the leader is before checking the
	// whole cluster's leader view (replication to the fresh peer retries with
	// backoff, so this settles asynchronously).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		laddr, _ := c1.rafts[0].LeaderWithID()
		if laddr == leaderAddr {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.EnsureLeader(t, leaderAddr)

	// The learner must not be a voter yet.
	cfgFut := leader.GetConfiguration()
	require.NoError(t, cfgFut.Error())
	cfg := cfgFut.Configuration()
	require.Len(t, cfg.Servers, 2)
	var learnerSuffrage bool
	for _, s := range cfg.Servers {
		if s.ID == c1.rafts[0].localID {
			learnerSuffrage = s.Suffrage == Voter
		}
	}
	require.False(t, learnerSuffrage, "learner must not be a voter before promotion")

	// Promote once it catches up.
	promoteFut := leader.PromoteToVoter(c1.rafts[0].localID, 15*time.Second)
	require.NoError(t, promoteFut.Error())
	require.Greater(t, promoteFut.Index(), uint64(0))

	// Now it must be a voter, and a later config must have two voters.
	cfgFut = leader.GetConfiguration()
	require.NoError(t, cfgFut.Error())
	cfg = cfgFut.Configuration()
	voters := 0
	for _, s := range cfg.Servers {
		if s.Suffrage == Voter {
			voters++
		}
	}
	require.Equal(t, 2, voters)
}

// TestVraft_NoopFlushIdleLeader verifies the 12.4 fallback: a newly elected
// leader with no client writes still commits its leadership noop within the
// flush window, so the term commit is established in idle clusters.
func TestVraft_NoopFlushIdleLeader(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()

	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// No client writes. The pending noop must be flushed on its own and commit.
	waitForCommitIndex(t, leader, 1, 5*time.Second)
}

// TestVraft_NoopMergeFirstWrite verifies a write issued immediately after
// leadership succeeds and commits (regression guard for the 12.4 noop merge:
// the leadership noop shares the first batch instead of stalling writes).
func TestVraft_NoopMergeFirstWrite(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()

	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	fut := leader.Apply([]byte("first-write"), 0)
	require.NoError(t, fut.Error())
	require.Greater(t, fut.Index(), uint64(0))
	waitForCommitIndex(t, leader, fut.Index(), 5*time.Second)
}
