package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/vaibhav-prk/quorum/internal/raft"
)

// Network is a shared in-process registry of nodes.
// All nodes registered here can communicate directly — no sockets needed.
// Used for tests only; swap for the gRPC transport in production.
type Network struct {
	mu      sync.RWMutex
	nodes   map[int64]*raft.Node
	dropped map[peerPair]bool // pairs whose messages are silently dropped
}

type peerPair struct{ from, to int64 }

func NewNetwork() *Network {
	return &Network{
		nodes:   make(map[int64]*raft.Node),
		dropped: make(map[peerPair]bool),
	}
}

// Register adds a node to the network so other nodes can reach it.
func (net *Network) Register(id int64, node *raft.Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = node
}

// Isolate makes all messages to/from id return an error (simulates crash/partition).
func (net *Network) Isolate(id int64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	for other := range net.nodes {
		net.dropped[peerPair{id, other}] = true
		net.dropped[peerPair{other, id}] = true
	}
}

// Heal restores all connections.
func (net *Network) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.dropped = make(map[peerPair]bool)
}

// Transport is one node's view of the network.
// Implements raft.Transport.
type InMemTransport struct {
	id  int64
	net *Network
}

func (net *Network) TransportFor(id int64) *InMemTransport {
	return &InMemTransport{id: id, net: net}
}

func (t *InMemTransport) RequestVote(ctx context.Context, peer int64, req *raft.VoteRequest) (*raft.VoteReply, error) {
	t.net.mu.RLock()
	node, ok := t.net.nodes[peer]
	dropped := t.net.dropped[peerPair{t.id, peer}]
	t.net.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("inmem: node %d not registered", peer)
	}
	if dropped {
		return nil, fmt.Errorf("inmem: link %d→%d is down", t.id, peer)
	}

	// Respect context cancellation.
	done := make(chan *raft.VoteReply, 1)
	go func() { done <- node.HandleRequestVote(req) }()

	select {
	case reply := <-done:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *InMemTransport) AppendEntries(ctx context.Context, peer int64, req *raft.AppendRequest) (*raft.AppendReply, error) {
	t.net.mu.RLock()
	node, ok := t.net.nodes[peer]
	dropped := t.net.dropped[peerPair{t.id, peer}]
	t.net.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("inmem: node %d not registered", peer)
	}
	if dropped {
		return nil, fmt.Errorf("inmem: link %d→%d is down", t.id, peer)
	}

	done := make(chan *raft.AppendReply, 1)
	go func() { done <- node.HandleAppendEntries(req) }()

	select {
	case reply := <-done:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
