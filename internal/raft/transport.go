package raft

import "context"

type Transport interface {
	RequestVote(ctx context.Context, peer int64, req *VoteRequest) (*VoteReply, error)
	AppendEntries(ctx context.Context, peer int64, req *AppendRequest) (*AppendReply, error)
}
