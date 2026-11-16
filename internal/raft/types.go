package raft

import "github.com/vaibhav-prk/quorum/internal/storage"

type State uint8

const (
	Follower  State = iota
	Candidate
	Leader
)

type VoteRequest struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendRequest struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []storage.Entry
	LeaderCommit uint64
}

type AppendReply struct {
	Term    uint64
	Success bool
}
