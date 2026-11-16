package raft

import (
	"encoding/binary"
	"errors"
	"os"
)

// persist.go stores the two fields Raft requires to survive a restart:
//   - currentTerm  (monotonically increasing, never go back)
//   - votedFor     (who we voted for in currentTerm; 0 = nobody)
//
// Format (all big-endian):
//
// | term (8) | votedFor (8) |  — fixed 16-byte file, overwritten in place.

const (
	persistFile = "raft-state"
	persistSize = 16
)

type persister struct {
	f *os.File
}

func openPersister(dir string) (*persister, error) {
	path := dir + "/" + persistFile
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &persister{f: f}, nil
}

// load reads term and votedFor from disk.
// A brand-new (empty) file returns 0, 0 — correct initial Raft state.
func (p *persister) load() (term, votedFor uint64, err error) {
	buf := make([]byte, persistSize)
	n, err := p.f.ReadAt(buf, 0)

	if err != nil || n < persistSize {
		if errors.Is(err, os.ErrNotExist) || n == 0 {
			return 0, 0, nil // fresh node
		}
		// partial read → treat as fresh (crash before first save)
		return 0, 0, nil
	}

	term = binary.BigEndian.Uint64(buf[0:8])
	votedFor = binary.BigEndian.Uint64(buf[8:16])
	return term, votedFor, nil
}

// save flushes term and votedFor to disk.
// changes these fields — this is a hard Raft correctness requirement.
func (p *persister) save(term, votedFor uint64) error {
	var buf [persistSize]byte
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], votedFor)

	if _, err := p.f.WriteAt(buf[:], 0); err != nil {
		return err
	}
	return p.f.Sync()
}

func (p *persister) close() error {
	return p.f.Close()
}
