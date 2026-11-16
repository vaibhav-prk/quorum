package raft

import "github.com/vaibhav-prk/quorum/internal/storage"

type FSM interface {
	Apply(entry storage.Entry)
}
