package raft_test

import (
	"testing"
	"time"

	"github.com/vaibhav-prk/quorum/internal/raft"
	"github.com/vaibhav-prk/quorum/internal/storage"
	"github.com/vaibhav-prk/quorum/internal/transport"
)

// noopFSM satisfies raft.FSM for tests that don't care about applied entries.
type noopFSM struct{}

func (noopFSM) Apply(_ storage.Entry) {}

// cluster spins up n nodes connected via an in-memory network.
type cluster struct {
	nodes   []*raft.Node
	ids     []int64
	network *transport.Network
	t       *testing.T
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	net := transport.NewNetwork()
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}

	nodes := make([]*raft.Node, n)
	for i, id := range ids {
		peers := make([]int64, 0, n-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		tr := net.TransportFor(id)
		node, err := raft.NewNode(id, peers, t.TempDir(), tr, noopFSM{})
		if err != nil {
			t.Fatalf("NewNode %d: %v", id, err)
		}
		net.Register(id, node)
		nodes[i] = node
	}

	for _, node := range nodes {
		node.Start()
	}

	return &cluster{nodes: nodes, ids: ids, network: net, t: t}
}

func (c *cluster) close() {
	for _, n := range c.nodes {
		n.Close()
	}
}

// leaders returns all nodes currently in Leader state.
func (c *cluster) leaders() []*raft.Node {
	var out []*raft.Node
	for _, n := range c.nodes {
		if n.State() == raft.Leader {
			out = append(out, n)
		}
	}
	return out
}

// waitLeader polls until exactly one leader is elected or the deadline passes.
func (c *cluster) waitLeader(timeout time.Duration) *raft.Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ls := c.leaders(); len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected within %v", timeout)
	return nil
}

// ---- tests ----

func TestOneLeaderElected(t *testing.T) {
	c := newCluster(t, 3)
	defer c.close()

	leader := c.waitLeader(2 * time.Second)
	t.Logf("leader elected: node %v term %d", leader.State(), leader.Term())

	// Confirm no split-brain — still exactly one leader after a quiet period.
	time.Sleep(300 * time.Millisecond)
	if ls := c.leaders(); len(ls) != 1 {
		t.Fatalf("want 1 leader, got %d", len(ls))
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	defer c.close()

	first := c.waitLeader(2 * time.Second)
	t.Logf("first leader elected (term %d)", first.Term())

	// Isolate the leader — it can no longer reach peers.
	c.network.Isolate(first.ID())

	// A new leader must emerge from the remaining two nodes.
	c.waitLeader(3 * time.Second)
}
