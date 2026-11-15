package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func mkEntry(i uint64) Entry {
	return Entry{Index: i, Term: 1, Payload: []byte(fmt.Sprintf("payload-%d", i))}
}

func newSeg(t *testing.T, base uint64) (*Segment, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.log")
	s, err := openSegment(path, base)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func fill(t *testing.T, s *Segment, from uint64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := s.Append(mkEntry(from + uint64(i))); err != nil {
			t.Fatalf("append %d: %v", from+uint64(i), err)
		}
	}
}

func reopen(t *testing.T, s *Segment, path string, base uint64) *Segment {
	t.Helper()
	s.file.Close()
	s2, err := openSegment(path, base)
	if err != nil {
		t.Fatal(err)
	}
	return s2
}

func checkRange(t *testing.T, s *Segment, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		e, err := s.Read(i)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		want := mkEntry(i)
		if e.Index != want.Index || e.Term != want.Term || !bytes.Equal(e.Payload, want.Payload) {
			t.Fatalf("entry %d mismatch: got %+v", i, e)
		}
	}
}

func TestAppendRead(t *testing.T) {
	s, _ := newSeg(t, 100)
	fill(t, s, 100, 50)
	checkRange(t, s, 100, 149)
	if last, ok := s.LastIndex(); !ok || last != 149 {
		t.Fatalf("LastIndex = %d,%v", last, ok)
	}
}

func TestEmptyLastIndex(t *testing.T) {
	s, _ := newSeg(t, 1)
	if _, ok := s.LastIndex(); ok {
		t.Fatal("empty segment should report ok=false")
	}
}

func TestAppendBadIndex(t *testing.T) {
	s, _ := newSeg(t, 10)
	fill(t, s, 10, 3)
	if err := s.Append(mkEntry(14)); err != ErrBadIndex { // gap
		t.Fatalf("gap: got %v", err)
	}
	if err := s.Append(mkEntry(12)); err != ErrBadIndex { // duplicate
		t.Fatalf("dup: got %v", err)
	}
	checkRange(t, s, 10, 12) // nothing damaged
}

func TestReadOutOfRange(t *testing.T) {
	s, _ := newSeg(t, 10)
	fill(t, s, 10, 5)
	for _, i := range []uint64{0, 9, 15, 1000} {
		if _, err := s.Read(i); err != ErrOutOfRange {
			t.Fatalf("read %d: got %v", i, err)
		}
	}
}

// Catches the s.size bug: append after reopen must not overwrite.
func TestAppendAfterReopen(t *testing.T) {
	s, path := newSeg(t, 1)
	fill(t, s, 1, 100)
	s = reopen(t, s, path, 1)
	fill(t, s, 101, 100)
	checkRange(t, s, 1, 200)
	s = reopen(t, s, path, 1)
	checkRange(t, s, 1, 200)
}

func TestReopen10k(t *testing.T) {
	s, path := newSeg(t, 1)
	fill(t, s, 1, 10000)
	s = reopen(t, s, path, 1)
	checkRange(t, s, 1, 10000)
}

func TestTruncateAtEndIsNoop(t *testing.T) {
	s, _ := newSeg(t, 1)
	fill(t, s, 1, 10)
	if err := s.TruncateFrom(11); err != nil {
		t.Fatalf("got %v", err)
	}
	checkRange(t, s, 1, 10)
}

func TestTruncateBounds(t *testing.T) {
	s, _ := newSeg(t, 10)
	fill(t, s, 10, 5)
	if err := s.TruncateFrom(9); err != ErrOutOfRange {
		t.Fatalf("below base: got %v", err)
	}
	if err := s.TruncateFrom(16); err != ErrOutOfRange {
		t.Fatalf("past end: got %v", err)
	}
	checkRange(t, s, 10, 14)
}

func TestTruncateMiddleThenAppend(t *testing.T) {
	s, path := newSeg(t, 1)
	fill(t, s, 1, 20)
	if err := s.TruncateFrom(11); err != nil {
		t.Fatal(err)
	}
	if last, _ := s.LastIndex(); last != 10 {
		t.Fatalf("last = %d", last)
	}
	if _, err := s.Read(11); err != ErrOutOfRange {
		t.Fatalf("read truncated: %v", err)
	}
	// new entries replace the truncated ones
	for i := uint64(11); i <= 15; i++ {
		e := Entry{Index: i, Term: 2, Payload: []byte("new")}
		if err := s.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	s = reopen(t, s, path, 1)
	checkRange(t, s, 1, 10)
	e, err := s.Read(13)
	if err != nil || e.Term != 2 || string(e.Payload) != "new" {
		t.Fatalf("got %+v %v", e, err)
	}
	if last, _ := s.LastIndex(); last != 15 {
		t.Fatalf("last after reopen = %d", last)
	}
}

func TestTruncateToBase(t *testing.T) {
	s, path := newSeg(t, 5)
	fill(t, s, 5, 10)
	if err := s.TruncateFrom(5); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.LastIndex(); ok {
		t.Fatal("should be empty")
	}
	fill(t, s, 5, 3) // can restart from base
	s = reopen(t, s, path, 5)
	checkRange(t, s, 5, 7)
}

// Simulated crash: cut the file at every byte offset in the last few entries.
func TestTornTail(t *testing.T) {
	s, path := newSeg(t, 1)
	fill(t, s, 1, 20)
	full := s.size
	s.file.Close()
	orig, _ := os.ReadFile(path)

	start := full - 200 // covers several entries' worth of cut points
	for cut := start; cut <= full; cut++ {
		if err := os.WriteFile(path, orig[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		s2, err := openSegment(path, 1)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		last, ok := s2.LastIndex()
		if !ok {
			t.Fatalf("cut %d: empty", cut)
		}
		checkRange(t, s2, 1, last) // clean prefix
		info, _ := os.Stat(path)
		if info.Size() != s2.size {
			t.Fatalf("cut %d: file %d != size %d", cut, info.Size(), s2.size)
		}
		if err := s2.Append(mkEntry(last + 1)); err != nil { // writable afterwards
			t.Fatalf("cut %d: append: %v", cut, err)
		}
		s2.file.Close()
	}
}

func TestCorruptMiddleDropsTail(t *testing.T) {
	s, path := newSeg(t, 1)
	fill(t, s, 1, 20)
	k := 9 // entry index 10
	pos := s.offsets[k]
	s.file.Close()

	data, _ := os.ReadFile(path)
	data[pos+headerSize] ^= 0xFF // flip a body byte
	os.WriteFile(path, data, 0o644)

	s2, err := openSegment(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if last, _ := s2.LastIndex(); last != 9 {
		t.Fatalf("last = %d, want 9", last)
	}
	checkRange(t, s2, 1, 9)
}

func TestCorruptHeaderLength(t *testing.T) {
	s, path := newSeg(t, 1)
	fill(t, s, 1, 5)
	pos := s.offsets[2]
	s.file.Close()

	data, _ := os.ReadFile(path)
	copy(data[pos+4:pos+8], []byte{0xFF, 0xFF, 0xFF, 0xFF}) // absurd length
	os.WriteFile(path, data, 0o644)

	s2, err := openSegment(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if last, _ := s2.LastIndex(); last != 2 {
		t.Fatalf("last = %d, want 2", last)
	}
}
