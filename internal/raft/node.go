package raft

import (
	"fmt"
	"sync"
	"time"

	"github.com/vaibhav-prk/quorum/internal/storage"
)

type Node struct {
	mu      sync.Mutex
	id      uint64
	peers   []uint64
	dataDir string

	// Persistent state (must be saved before replying to RPCs)
	currentTerm uint64
	votedFor    uint64
	log         *storage.Log
	persist     *persister

	// Volatile state
	state       State
	commitIndex uint64
	lastApplied uint64

	// Leader-only (re-initialised on each election win)
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Pluggable
	transport Transport
	fsm       FSM

	// Timing
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// NewNode creates a Node, opens (or recovers) its log and persisted state,
// and starts as a Follower. Call node.Start() to begin the election loop.
func NewNode(id uint64, peers []uint64, dataDir string, transport Transport, fsm FSM) (*Node, error) {
	// Open the durable log.
	log, err := storage.Open(dataDir, storage.Options{})
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}

	// Open (or create) the term/votedFor persister.
	p, err := openPersister(dataDir)
	if err != nil {
		log.Close()
		return nil, fmt.Errorf("raft: open persister: %w", err)
	}

	// Restore persisted state — if the file is brand-new, returns 0, 0.
	term, votedFor, err := p.load()
	if err != nil {
		log.Close()
		p.close()
		return nil, fmt.Errorf("raft: load persisted state: %w", err)
	}

	n := &Node{
		id:          id,
		peers:       peers,
		dataDir:     dataDir,
		currentTerm: term,
		votedFor:    votedFor,
		log:         log,
		persist:     p,
		state:       Follower,
		transport:   transport,
		fsm:         fsm,
	}

	return n, nil
}

// Close shuts down the node cleanly.
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	if err := n.log.Close(); err != nil {
		return err
	}
	return n.persist.close()
}
