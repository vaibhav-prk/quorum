package raft

// applyLoop runs in the background and applies committed log entries to the FSM.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}

		for {
			select {
			case <-n.stopCh:
				return
			default:
			}

			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}

			start := n.lastApplied + 1
			end := n.commitIndex
			entries, err := n.log.Range(start, end+1)
			n.mu.Unlock()

			if err != nil {
				// Failed to read range from log. Can't proceed with this batch.
				break
			}

			for _, entry := range entries {
				select {
				case <-n.stopCh:
					return
				default:
				}

				if n.fsm != nil {
					n.fsm.Apply(entry)
				}

				n.mu.Lock()
				n.lastApplied = entry.Index
				n.mu.Unlock()
			}
		}
	}
}

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied returns the highest log index applied to the state machine.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}
