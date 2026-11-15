package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultMaxSegmentEntries = 10_000
	defaultMaxSegmentBytes   = 64 << 20
	segmentExt               = ".log"
)

type Options struct {
	MaxSegmentEntries int   // roll when the active segment reaches this many entries
	MaxSegmentBytes   int64 // or this many bytes
}

// Log is an ordered, durable sequence of entries spread over segment files.
// Raft indexes start at 1. The last segment is the only one that accepts writes.
type Log struct {
	mu       sync.Mutex
	dir      string
	opts     Options
	segments []*Segment // sorted by baseIndex, never empty after Open
	closed   bool
}

func Open(dir string, opts Options) (*Log, error) {
	if opts.MaxSegmentEntries <= 0 {
		opts.MaxSegmentEntries = defaultMaxSegmentEntries
	}
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	bases, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	l := &Log{dir: dir, opts: opts}

	for _, base := range bases {
		s, err := openSegment(l.segPath(base), base)
		if err != nil {
			l.closeAll()
			return nil, err
		}
		l.segments = append(l.segments, s)
	}

	if len(l.segments) == 0 {
		s, err := openSegment(l.segPath(1), 1)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, s)
		if err := syncDir(dir); err != nil {
			l.closeAll()
			return nil, err
		}
	}

	// Segments must line up exactly. A gap means a sealed segment lost entries
	// (recover() truncated it) or a file is missing.
	for i := 1; i < len(l.segments); i++ {
		prev := l.segments[i-1]
		if prev.baseIndex+uint64(len(prev.offsets)) != l.segments[i].baseIndex {
			l.closeAll()
			return nil, fmt.Errorf("%w: gap or overlap before segment %d", ErrCorrupt, l.segments[i].baseIndex)
		}
	}

	return l, nil
}

func (l *Log) Append(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	if len(e.Payload) > maxBodySize-fixedBody {
		return ErrTooLarge
	}
	if e.Index != l.lastIndex()+1 {
		return ErrBadIndex
	}

	active := l.active()
	// Never roll an empty segment, or you create empty files forever.
	if len(active.offsets) > 0 &&
		(len(active.offsets) >= l.opts.MaxSegmentEntries || active.size >= l.opts.MaxSegmentBytes) {
		if err := l.roll(e.Index); err != nil {
			return err
		}
		active = l.active()
	}

	return active.Append(e)
}

func (l *Log) Read(index uint64) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return Entry{}, ErrClosed
	}
	if index < l.firstIndex() || index > l.lastIndex() {
		return Entry{}, ErrOutOfRange
	}
	return l.segments[l.find(index)].Read(index)
}

// Range returns entries in [lo, hi).
func (l *Log) Range(lo, hi uint64) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil, ErrClosed
	}
	if lo > hi || lo < l.firstIndex() || hi > l.lastIndex()+1 {
		return nil, ErrOutOfRange
	}

	out := make([]Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		e, err := l.segments[l.find(i)].Read(i)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// TruncateFrom removes index and everything after it. index == lastIndex+1 is a no-op.
func (l *Log) TruncateFrom(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	next := l.lastIndex() + 1
	if index < l.firstIndex() || index > next {
		return ErrOutOfRange
	}
	if index == next {
		return nil
	}

	i := l.find(index)

	// Delete later segments newest-first, THEN cut segment i. A crash in between
	// leaves a valid (longer) log. The reverse order would leave a gap.
	for j := len(l.segments) - 1; j > i; j-- {
		s := l.segments[j]
		_ = s.file.Close()
		if err := os.Remove(s.path); err != nil {
			return err
		}
		l.segments = l.segments[:j]
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}

	return l.segments[i].TruncateFrom(index)
}

func (l *Log) FirstIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstIndex()
}

// LastIndex returns 0 for an empty log.
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndex()
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.active().file.Sync()
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.closeAll()
}

// ---- helpers (caller holds l.mu) ----

func (l *Log) active() *Segment { return l.segments[len(l.segments)-1] }

func (l *Log) firstIndex() uint64 { return l.segments[0].baseIndex }

func (l *Log) lastIndex() uint64 {
	s := l.active()

	index, ok := s.LastIndex()
	if !ok {
		return s.baseIndex - 1
	}

	return index
}

// find returns the position of the last segment with baseIndex <= index.
func (l *Log) find(index uint64) int {
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseIndex > index
	})
	return i - 1
}

func (l *Log) roll(nextIndex uint64) error {
	s, err := openSegment(l.segPath(nextIndex), nextIndex)
	if err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		_ = s.file.Close()
		return err
	}
	l.segments = append(l.segments, s)
	return nil
}

func (l *Log) closeAll() error {
	var first error
	for _, s := range l.segments {
		if err := s.file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (l *Log) segPath(base uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%020d%s", base, segmentExt))
}

func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, de := range ents {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, segmentExt) {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(name, segmentExt), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("storage: bad segment file name %q: %w", name, err)
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
