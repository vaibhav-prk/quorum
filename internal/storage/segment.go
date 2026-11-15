package storage

import (
	"encoding/binary"
	"os"
	"sync"
)

// Segment manages one log file. This struct is in-memory only; it is never written to disk.
type Segment struct {
	mu        sync.RWMutex
	file      *os.File
	path      string
	baseIndex uint64  // index of the first entry in this file (also in its filename)
	offsets   []int64 // offsets[i] = byte position of entry (baseIndex + i)
	size      int64   // current end-of-file position, where the next write goes
}

func openSegment(path string, baseIndex uint64) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	s := &Segment{file: f, path: path, baseIndex: baseIndex}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}

	return s, nil
}

func (s *Segment) recover() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}

	fileSize := info.Size()

	var pos int64
	hdr := make([]byte, headerSize)

	for pos < fileSize {
		if fileSize-pos < headerSize {
			break
		}

		if _, err := s.file.ReadAt(hdr, pos); err != nil {
			return err
		}

		bodyLen := int64(binary.BigEndian.Uint32(hdr[4:8]))
		if bodyLen < fixedBody || bodyLen > maxBodySize {
			break
		}

		total := headerSize + bodyLen
		if pos+total > fileSize {
			break
		}

		buf := make([]byte, total)
		if _, err := s.file.ReadAt(buf, pos); err != nil {
			return err
		}

		e, err := decode(buf)
		if err != nil {
			break
		}

		if e.Index != s.baseIndex+uint64(len(s.offsets)) {
			break
		}

		s.offsets = append(s.offsets, pos)
		pos += total
	}

	if pos < fileSize {
		if err := s.file.Truncate(pos); err != nil {
			return err
		}

		if err := s.file.Sync(); err != nil {
			return err
		}
	}

	s.size = pos

	return nil
}

// Append
func (s *Segment) Append(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e.Index != s.baseIndex+uint64(len(s.offsets)) {
		return ErrBadIndex
	}

	buf := encode(e)

	if _, err := s.file.WriteAt(buf, s.size); err != nil {
		return err
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	s.offsets = append(s.offsets, s.size)
	s.size += int64(len(buf))

	return nil
}

func (s *Segment) Read(index uint64) (Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.baseIndex || index >= s.baseIndex+uint64(len(s.offsets)) {
		return Entry{}, ErrOutOfRange
	}

	pos := s.offsets[index-s.baseIndex]

	hdr := make([]byte, headerSize)
	if _, err := s.file.ReadAt(hdr, pos); err != nil {
		return Entry{}, err
	}

	bodyLen := int64(binary.BigEndian.Uint32(hdr[4:8]))
	if bodyLen < fixedBody || bodyLen > maxBodySize {
		return Entry{}, ErrCorrupt
	}

	buf := make([]byte, headerSize+bodyLen)
	if _, err := s.file.ReadAt(buf, pos); err != nil {
		return Entry{}, err
	}

	return decode(buf)
}

// TruncateFrom removes the entry at index and everything after it.
func (s *Segment) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	end := s.baseIndex + uint64(len(s.offsets))
	if index < s.baseIndex || index > end {
		return ErrOutOfRange
	}

	if index == end {
		return nil
	}

	i := index - s.baseIndex
	pos := s.offsets[i]

	if err := s.file.Truncate(pos); err != nil {
		return err
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	s.offsets = s.offsets[:i]
	s.size = pos

	return nil
}

// LastIndex returns the index of the last Entry. ok is false if Segment is empty
func (s *Segment) LastIndex() (index uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.offsets) == 0 {
		return 0, false
	}

	return s.baseIndex + uint64(len(s.offsets)-1), true
}
