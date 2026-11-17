package raft_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vaibhav-prk/quorum/internal/raft"
	"github.com/vaibhav-prk/quorum/internal/storage"
	"github.com/vaibhav-prk/quorum/internal/transport"
)

type recordingFSM struct {
	mu      sync.Mutex
	entries []storage.Entry
}

func (f *recordingFSM) Apply(e storage.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, e)
}

func (f *recordingFSM) getEntries() []storage.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storage.Entry, len(f.entries))
	copy(out, f.entries)
	return out
}

type replicationCluster struct {
	nodes   []*raft.Node
	fsms    []*recordingFSM
	network *transport.Network
	t       *testing.T
}

func newReplicationCluster(t *testing.T, n int) *replicationCluster {
	t.Helper()
	net := transport.NewNetwork()
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}

	nodes := make([]*raft.Node, n)
	fsms := make([]*recordingFSM, n)

	for i, id := range ids {
		peers := make([]int64, 0, n-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		fsms[i] = &recordingFSM{}
		tr := net.TransportFor(id)
		node, err := raft.NewNode(id, peers, t.TempDir(), tr, fsms[i])
		if err != nil {
			t.Fatalf("NewNode %d: %v", id, err)
		}
		net.Register(id, node)
		nodes[i] = node
	}

	for _, node := range nodes {
		node.Start()
	}

	return &replicationCluster{
		nodes:   nodes,
		fsms:    fsms,
		network: net,
		t:       t,
	}
}

func (c *replicationCluster) close() {
	for _, n := range c.nodes {
		_ = n.Close()
	}
}

func (c *replicationCluster) waitLeader(timeout time.Duration) *raft.Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*raft.Node
		for _, n := range c.nodes {
			if n.State() == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected within %v", timeout)
	return nil
}

func (c *replicationCluster) waitApplied(idx uint64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, f := range c.fsms {
			entries := f.getEntries()
			if len(entries) < int(idx) {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for all nodes to apply index %d", idx)
}

func TestReplicationBasic(t *testing.T) {
	c := newReplicationCluster(t, 3)
	defer c.close()

	leader := c.waitLeader(2 * time.Second)
	payload := []byte("hello raft")

	if err := leader.Append(payload); err != nil {
		t.Fatalf("Append: %v", err)
	}

	c.waitApplied(1, 3*time.Second)

	for i, f := range c.fsms {
		entries := f.getEntries()
		if len(entries) != 1 {
			t.Fatalf("node %d has %d applied entries, want 1", i+1, len(entries))
		}
		if !bytes.Equal(entries[0].Payload, payload) {
			t.Fatalf("node %d entry payload %q, want %q", i+1, entries[0].Payload, payload)
		}
		if entries[0].Index != 1 {
			t.Fatalf("node %d entry index %d, want 1", i+1, entries[0].Index)
		}
	}
}

func TestReplicationMultipleEntries(t *testing.T) {
	c := newReplicationCluster(t, 3)
	defer c.close()

	leader := c.waitLeader(2 * time.Second)
	nEntries := 10

	for i := 1; i <= nEntries; i++ {
		payload := []byte(fmt.Sprintf("msg-%d", i))
		if err := leader.Append(payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	c.waitApplied(uint64(nEntries), 4*time.Second)

	for i, f := range c.fsms {
		entries := f.getEntries()
		if len(entries) != nEntries {
			t.Fatalf("node %d has %d entries, want %d", i+1, len(entries), nEntries)
		}
		for j, e := range entries {
			expected := []byte(fmt.Sprintf("msg-%d", j+1))
			if !bytes.Equal(e.Payload, expected) {
				t.Fatalf("node %d entry %d payload %q, want %q", i+1, j, e.Payload, expected)
			}
			if e.Index != uint64(j+1) {
				t.Fatalf("node %d entry %d index %d, want %d", i+1, j, e.Index, j+1)
			}
		}
	}
}

func TestReplicationNonLeaderFails(t *testing.T) {
	c := newReplicationCluster(t, 3)
	defer c.close()

	leader := c.waitLeader(2 * time.Second)

	var follower *raft.Node
	for _, n := range c.nodes {
		if n.ID() != leader.ID() {
			follower = n
			break
		}
	}

	err := follower.Append([]byte("should fail"))
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("Append on follower: got %v, want ErrNotLeader", err)
	}
}

func TestReplicationFollowerCatchUp(t *testing.T) {
	c := newReplicationCluster(t, 3)
	defer c.close()

	leader := c.waitLeader(2 * time.Second)

	// Pick a follower to isolate.
	var isolatedID int64
	var isolatedFSMIndex int
	for i, n := range c.nodes {
		if n.ID() != leader.ID() {
			isolatedID = n.ID()
			isolatedFSMIndex = i
			break
		}
	}

	c.network.Isolate(isolatedID)

	// Append 3 entries while follower is isolated.
	for i := 1; i <= 3; i++ {
		if err := leader.Append([]byte(fmt.Sprintf("entry-%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Wait for majority (the other 2 nodes) to commit and apply.
	time.Sleep(300 * time.Millisecond)

	// Isolated node should not have applied yet.
	if applied := len(c.fsms[isolatedFSMIndex].getEntries()); applied != 0 {
		t.Fatalf("isolated node applied %d entries, want 0", applied)
	}

	// Reconnect isolated node.
	c.network.Heal()

	// Wait for the caught up node to apply all 3 entries.
	c.waitApplied(3, 4*time.Second)

	entries := c.fsms[isolatedFSMIndex].getEntries()
	if len(entries) != 3 {
		t.Fatalf("reconnected node has %d entries, want 3", len(entries))
	}
}
