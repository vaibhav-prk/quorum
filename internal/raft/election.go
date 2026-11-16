package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// becomeFollower steps down to Follower, updates term, clears vote.
// Must be called with n.mu held.
func (n *Node) becomeFollower(term uint64) {
	stopTimer(n.heartbeatTimer)
	n.heartbeatTimer = nil

	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	_ = n.persist.save(term, -1)

	n.resetElectionTimer()
}

func stopTimer(t *time.Timer) {
	if t == nil {
		return
	}

	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func (n *Node) resetElectionTimer() {
	stopTimer(n.electionTimer)
	timeout := randomElectionTimeout()
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.startElection()
	})
}

// startElection is triggered when the election timer fires.
// Runs in its own goroutine (via AfterFunc); acquires the lock internally.
func (n *Node) startElection() {
	n.mu.Lock()

	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	peers := n.peers

	// Persist before sending any RPC — hard correctness requirement.
	if err := n.persist.save(term, n.id); err != nil {
		n.mu.Unlock()
		return
	}

	lastIndex := n.log.LastIndex()
	lastTerm := n.lastLogTerm(lastIndex)

	n.resetElectionTimer() // restart in case this election times out

	n.mu.Unlock()

	req := &VoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	var (
		mu    sync.Mutex
		votes = 1 // self-vote
	)
	majority := len(peers)/2 + 1

	for _, peer := range peers {
		go func(peer int64) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			reply, err := n.transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Higher term seen — step down immediately.
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			// Stale reply — term changed or we already stepped down.
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if !reply.VoteGranted {
				return
			}

			mu.Lock()
			votes++
			won := votes == majority // exactly at majority to call becomeLeader once
			mu.Unlock()

			if won {
				n.becomeLeader()
			}
		}(peer)
	}
}

// becomeLeader initialises leader state and starts heartbeats.
// Must be called with n.mu held.
func (n *Node) becomeLeader() {
	n.state = Leader
	stopTimer(n.electionTimer)
	n.electionTimer = nil

	nextIdx := n.log.LastIndex() + 1
	n.nextIndex = make(map[uint64]uint64, len(n.peers))
	n.matchIndex = make(map[uint64]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[uint64(peer)] = nextIdx
		n.matchIndex[uint64(peer)] = 0
	}

	n.sendHeartbeats()
	n.heartbeatTimer = time.AfterFunc(heartbeatInterval, n.heartbeatLoop)
}

// heartbeatLoop fires repeatedly while this node is Leader.
func (n *Node) heartbeatLoop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}
	n.sendHeartbeats()
	n.heartbeatTimer = time.AfterFunc(heartbeatInterval, n.heartbeatLoop)
}

// sendHeartbeats sends empty AppendEntries to all peers.
// Must be called with n.mu held.
func (n *Node) sendHeartbeats() {
	term := n.currentTerm
	leaderID := n.id

	for _, peer := range n.peers {
		go func(peer int64) {
			req := &AppendRequest{
				Term:     term,
				LeaderID: leaderID,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			reply, err := n.transport.AppendEntries(ctx, peer, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
			}
		}(peer)
	}
}

// lastLogTerm returns the term of the entry at lastIndex, or 0 if log is empty.
// Must be called with n.mu held.
func (n *Node) lastLogTerm(lastIndex uint64) uint64 {
	if lastIndex == 0 {
		return 0
	}
	e, err := n.log.Read(lastIndex)
	if err != nil {
		return 0
	}
	return e.Term
}

// Start begins the election loop. Call once after NewNode.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionTimer()
}
