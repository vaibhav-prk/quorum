package raft

// rpc.go — handlers called by the transport when an RPC arrives at this node.
// These are the only entry points into Raft state from the network.

// HandleRequestVote processes an incoming vote request from a candidate.
func (n *Node) HandleRequestVote(req *VoteRequest) *VoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &VoteReply{Term: n.currentTerm}

	// Candidate is behind — deny.
	if req.Term < n.currentTerm {
		return reply
	}

	// Candidate is ahead — step down and update term before deciding.
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		reply.Term = n.currentTerm
	}

	// Already voted for someone else this term — deny.
	if n.votedFor != -1 && n.votedFor != req.CandidateID {
		return reply
	}

	// Candidate's log must be at least as up-to-date as ours.
	// Rule: higher lastLogTerm wins; on tie, longer log wins.
	lastIndex := n.log.LastIndex()
	lastTerm := n.lastLogTerm(lastIndex)

	logOK := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	if !logOK {
		return reply
	}

	// Grant the vote.
	n.votedFor = req.CandidateID
	_ = n.persist.save(n.currentTerm, n.votedFor)
	n.resetElectionTimer() // reset so we don't start a competing election

	reply.VoteGranted = true
	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
// Handles both heartbeats (Entries == nil) and log replication.
func (n *Node) HandleAppendEntries(req *AppendRequest) *AppendReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendReply{Term: n.currentTerm}

	// Leader is behind — reject.
	if req.Term < n.currentTerm {
		return reply
	}

	// Valid leader — step down if needed and reset election timer.
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	} else {
		n.resetElectionTimer()
	}
	reply.Term = n.currentTerm

	// --- Step 1: consistency check ---
	// Our log must contain an entry at PrevLogIndex whose term matches PrevLogTerm.
	// PrevLogIndex == 0 means the leader is sending from the very beginning — always OK.
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > n.log.LastIndex() {
			return reply // Success = false
		}
		prev, err := n.log.Read(req.PrevLogIndex)
		if err != nil || prev.Term != req.PrevLogTerm {
			// Entry exists but term doesn't match — log diverged.
			return reply // Success = false
		}
	}

	// --- Step 2: conflict resolution + append ---
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1

		if idx <= n.log.LastIndex() {
			existing, err := n.log.Read(idx)
			if err != nil {
				return reply
			}
			if existing.Term != entry.Term {
				// Conflict — truncate from here onward, then append below.
				if err := n.log.TruncateFrom(idx); err != nil {
					return reply
				}
				// Fall through to append this and remaining entries.
				if err := n.log.Append(entry); err != nil {
					return reply
				}
			}
			// else: entry already present and matches — skip.
		} else {
			// Beyond our log — append.
			if err := n.log.Append(entry); err != nil {
				return reply
			}
		}
	}

	// --- Step 3: advance commitIndex ---
	if req.LeaderCommit > n.commitIndex {
		var lastIdx uint64
		if len(req.Entries) > 0 {
			lastIdx = req.PrevLogIndex + uint64(len(req.Entries))
		} else {
			lastIdx = n.log.LastIndex()
		}
		if lastIdx > n.commitIndex {
			n.commitIndex = min(req.LeaderCommit, lastIdx)
			n.signalApply()
		}
	}

	reply.Success = true
	return reply
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// signalApply wakes the apply loop. No-op until apply.go is wired up.
func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default: // already signalled, apply loop will catch up
	}
}
