package raft

import (
	"context"
	"sort"
	"time"

	"github.com/vaibhav-prk/quorum/internal/storage"
)

// Append writes a new entry to the leader's log and replicates it to all peers.
// Returns ErrNotLeader if this node is not the current leader.
// The entry is committed once a majority of nodes have stored it.
func (n *Node) Append(payload []byte) error {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	entry := storage.Entry{
		Index:   n.log.LastIndex() + 1,
		Term:    n.currentTerm,
		Payload: payload,
	}

	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()
		return err
	}

	// Update our own matchIndex — leader always has its own entries.
	n.matchIndex[uint64(n.id)] = entry.Index

	peers := n.peers
	n.mu.Unlock()

	// Replicate to all peers in parallel — don't hold the lock.
	for _, peer := range peers {
		go n.replicate(peer)
	}

	return nil
}

// replicate sends log entries to one peer, retrying with a decremented
// nextIndex until the follower's log converges with ours.
// Runs outside the lock; acquires it only when reading/writing node state.
func (n *Node) replicate(peer int64) {
	for {
		n.mu.Lock()

		if n.state != Leader {
			n.mu.Unlock()
			return
		}

		peerNextIdx := n.nextIndex[uint64(peer)]
		prevLogIndex := peerNextIdx - 1
		prevLogTerm := uint64(0)

		if prevLogIndex > 0 {
			prev, err := n.log.Read(prevLogIndex)
			if err != nil {
				n.mu.Unlock()
				return
			}
			prevLogTerm = prev.Term
		}

		// Send everything from nextIndex to end of log.
		lastIndex := n.log.LastIndex()
		var entries []storage.Entry
		if lastIndex >= peerNextIdx {
			var err error
			entries, err = n.log.Range(peerNextIdx, lastIndex+1)
			if err != nil {
				n.mu.Unlock()
				return
			}
		}

		req := &AppendRequest{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}

		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		reply, err := n.transport.AppendEntries(ctx, peer, req)
		cancel()

		if err != nil {
			return // peer unreachable — heartbeat loop will retry later
		}

		n.mu.Lock()

		// Higher term seen — step down immediately.
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
			n.mu.Unlock()
			return
		}

		// Stale reply — we're no longer leader or term changed.
		if n.state != Leader || n.currentTerm != req.Term {
			n.mu.Unlock()
			return
		}

		if reply.Success {
			// Follower accepted — advance its index tracking.
			newMatch := req.PrevLogIndex + uint64(len(req.Entries))
			if newMatch > n.matchIndex[uint64(peer)] {
				n.matchIndex[uint64(peer)] = newMatch
				n.nextIndex[uint64(peer)] = newMatch + 1
			}
			n.maybeCommit()
			n.mu.Unlock()
			return // done — no need to retry
		}

		// Follower rejected — log diverged. Decrement nextIndex and retry.
		if n.nextIndex[uint64(peer)] == req.PrevLogIndex+1 && n.nextIndex[uint64(peer)] > 1 {
			n.nextIndex[uint64(peer)]--
		} else if n.nextIndex[uint64(peer)] <= 1 {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		// Loop: retry with the decremented nextIndex.
	}
}

// maybeCommit advances commitIndex if a new index has been replicated
// to a majority of nodes (including the leader itself).
// Must be called with n.mu held.
func (n *Node) maybeCommit() {
	// Collect all matchIndex values (leader counts itself).
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.log.LastIndex()) // leader
	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[uint64(peer)])
	}

	// Sort descending — the median is the highest index held by a majority.
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	majority := matches[len(matches)/2]

	// Only commit entries from the current term (Raft Figure 8 safety rule).
	if majority > n.commitIndex {
		e, err := n.log.Read(majority)
		if err != nil || e.Term != n.currentTerm {
			return
		}
		n.commitIndex = majority
		n.signalApply()
	}
}
