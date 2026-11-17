package raft

import "errors"

var ErrNotLeader = errors.New("raft: not the leader")
