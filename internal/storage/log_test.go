package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ---- helpers ----

func openLog(t *testing.T, dir string, entries int) *Log {
	t.Helper()
	l, err := Open(dir, Options{MaxSegmentEntries: entries})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func appendN(t *testing.T, l *Log, from uint64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := l.Append(mkEntry(from + uint64(i))); err != nil {
			t.Fatalf("append %d: %v", from+uint64(i), err)
		}
	}
}

func verifyLog(t *testing.T, l *Log, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		e, err := l.Read(i)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !entriesEqual(e, mkEntry(i)) {
			t.Fatalf("entry %d mismatch: %+v", i, e)
		}
	}
}

func countSegFiles(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, de := range ents {
		if strings.HasSuffix(de.Name(), segmentExt) {
			n++
		}
	}
	return n
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*"+segmentExt))
	if err != nil {
		t.Fatal(err)
	}
	return m // Glob returns sorted names; names are zero-padded
}

// ---- open / basic ----

func TestOpenEmpty(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	defer l.Close()
	if l.FirstIndex() != 1 || l.LastIndex() != 0 {
		t.Fatalf("first=%d last=%d", l.FirstIndex(), l.LastIndex())
	}
	if _, err := l.Read(1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("got %v", err)
	}
	if countSegFiles(t, dir) != 1 {
		t.Fatal("expected exactly one segment file")
	}
}

func TestOpenCreatesMissingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b", "c")
	l := openLog(t, dir, 10)
	defer l.Close()
	appendN(t, l, 1, 3)
	verifyLog(t, l, 1, 3)
}

func TestAppendReadAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	defer l.Close()
	appendN(t, l, 1, 95)
	verifyLog(t, l, 1, 95)
	if l.LastIndex() != 95 {
		t.Fatalf("last = %d", l.LastIndex())
	}
	if got := countSegFiles(t, dir); got != 10 {
		t.Fatalf("segment files = %d, want 10", got)
	}
}

func TestLogReopen10k(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 100)
	appendN(t, l, 1, 10000)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l = openLog(t, dir, 100)
	defer l.Close()
	if l.FirstIndex() != 1 || l.LastIndex() != 10000 {
		t.Fatalf("first=%d last=%d", l.FirstIndex(), l.LastIndex())
	}
	verifyLog(t, l, 1, 10000)
	if got := countSegFiles(t, dir); got != 100 {
		t.Fatalf("segment files = %d, want 100", got)
	}
}

func TestLogAppendAfterReopen(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 25)
	l.Close()

	l = openLog(t, dir, 10)
	appendN(t, l, 26, 25)
	verifyLog(t, l, 1, 50)
	l.Close()

	l = openLog(t, dir, 10)
	defer l.Close()
	verifyLog(t, l, 1, 50)
}

func TestRollByBytes(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxSegmentEntries: 1 << 20, MaxSegmentBytes: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	appendN(t, l, 1, 50)
	verifyLog(t, l, 1, 50)
	if countSegFiles(t, dir) < 3 {
		t.Fatal("expected multiple segments from byte-based rolling")
	}
}

func TestSegmentFilesNamedByBaseIndex(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	defer l.Close()
	appendN(t, l, 1, 25)
	files := segFiles(t, dir)
	want := []string{l.segPath(1), l.segPath(11), l.segPath(21)}
	if len(files) != 3 {
		t.Fatalf("files = %v", files)
	}
	for i := range want {
		if files[i] != want[i] {
			t.Fatalf("file %d = %s, want %s", i, files[i], want[i])
		}
	}
}

// ---- append validation ----

func TestLogAppendBadIndex(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 5)

	for _, idx := range []uint64{0, 3, 5, 7, 100} { // old, duplicate, gap
		if err := l.Append(mkEntry(idx)); !errors.Is(err, ErrBadIndex) {
			t.Fatalf("index %d: got %v", idx, err)
		}
	}
	verifyLog(t, l, 1, 5)
	if l.LastIndex() != 5 {
		t.Fatalf("last = %d", l.LastIndex())
	}
}

func TestFirstAppendMustBeIndexOne(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	if err := l.Append(mkEntry(2)); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("got %v", err)
	}
	if err := l.Append(mkEntry(1)); err != nil {
		t.Fatal(err)
	}
}

func TestAppendTooLarge(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 2)

	big := Entry{Index: 3, Term: 1, Payload: make([]byte, maxBodySize)}
	if err := l.Append(big); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	if l.LastIndex() != 2 {
		t.Fatal("failed append changed the log")
	}
	appendN(t, l, 3, 1) // still usable
}

func TestAppendAtMaxSize(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 1)
	e := Entry{Index: 2, Term: 1, Payload: make([]byte, maxBodySize-fixedBody)}
	if err := l.Append(e); err != nil {
		t.Fatalf("entry exactly at the limit must be accepted: %v", err)
	}
	l.Close()

	// and it must survive recovery (recover() rejects bodyLen > maxBodySize)
	l = openLog(t, dir, 10)
	defer l.Close()
	if l.LastIndex() != 2 {
		t.Fatalf("max-size entry lost on reopen, last = %d", l.LastIndex())
	}
}

// ---- Range ----

func TestRange(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 35)

	got, err := l.Range(8, 25) // spans 3 segments
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 17 {
		t.Fatalf("len = %d", len(got))
	}
	for i, e := range got {
		if !entriesEqual(e, mkEntry(8+uint64(i))) {
			t.Fatalf("entry %d wrong: %+v", i, e)
		}
	}

	got, err = l.Range(1, 36) // whole log
	if err != nil || len(got) != 35 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}

	got, err = l.Range(10, 10) // empty
	if err != nil || len(got) != 0 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}

	got, err = l.Range(36, 36) // empty at the end
	if err != nil || len(got) != 0 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
}

func TestRangeOutOfRange(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 20)
	cases := [][2]uint64{{0, 5}, {5, 22}, {10, 5}, {21, 22}}
	for _, c := range cases {
		if _, err := l.Range(c[0], c[1]); !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("Range(%d,%d): got %v", c[0], c[1], err)
		}
	}
}

func TestLogReadOutOfRange(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 20)
	for _, i := range []uint64{0, 21, 1000} {
		if _, err := l.Read(i); !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("read %d: got %v", i, err)
		}
	}
}

// ---- truncate ----

func TestLogTruncateAtEndIsNoop(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 20)
	if err := l.TruncateFrom(21); err != nil {
		t.Fatalf("got %v", err)
	}
	verifyLog(t, l, 1, 20)
	if l.LastIndex() != 20 {
		t.Fatal("no-op truncate changed the log")
	}
}

func TestLogTruncateBounds(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 20)
	for _, i := range []uint64{0, 22, 1000} {
		if err := l.TruncateFrom(i); !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("TruncateFrom(%d): got %v", i, err)
		}
	}
	verifyLog(t, l, 1, 20)
}

// Segment bases are 1, 11, 21, 31, ... so test 1 before, on, and after a boundary.
func TestTruncateAroundSegmentBoundary(t *testing.T) {
	for _, at := range []uint64{10, 11, 12, 20, 21, 22, 30} {
		at := at
		t.Run(string(rune('0'+at/10))+"_"+string(rune('0'+at%10)), func(t *testing.T) {
			dir := t.TempDir()
			l := openLog(t, dir, 10)
			appendN(t, l, 1, 35)

			if err := l.TruncateFrom(at); err != nil {
				t.Fatal(err)
			}
			if l.LastIndex() != at-1 {
				t.Fatalf("last = %d, want %d", l.LastIndex(), at-1)
			}
			verifyLog(t, l, 1, at-1)
			if _, err := l.Read(at); !errors.Is(err, ErrOutOfRange) {
				t.Fatalf("read truncated: %v", err)
			}

			// new entries with a different term replace the old ones
			for i := at; i < at+15; i++ {
				e := Entry{Index: i, Term: 2, Payload: []byte("new")}
				if err := l.Append(e); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			last := at + 14

			check := func(l *Log) {
				verifyLog(t, l, 1, at-1)
				for i := at; i <= last; i++ {
					e, err := l.Read(i)
					if err != nil || e.Term != 2 || string(e.Payload) != "new" {
						t.Fatalf("entry %d: %+v %v", i, e, err)
					}
				}
				if l.LastIndex() != last {
					t.Fatalf("last = %d, want %d", l.LastIndex(), last)
				}
			}
			check(l)
			l.Close()

			l = openLog(t, dir, 10)
			defer l.Close()
			check(l)
		})
	}
}

func TestTruncateRemovesLaterSegmentFiles(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	defer l.Close()
	appendN(t, l, 1, 55) // segments: 1,11,21,31,41,51
	if countSegFiles(t, dir) != 6 {
		t.Fatalf("files = %d", countSegFiles(t, dir))
	}
	if err := l.TruncateFrom(25); err != nil {
		t.Fatal(err)
	}
	if got := countSegFiles(t, dir); got != 3 { // 1, 11, 21
		t.Fatalf("files after truncate = %d, want 3", got)
	}
}

func TestTruncateToFirstEmptiesLog(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 35)

	if err := l.TruncateFrom(1); err != nil {
		t.Fatal(err)
	}
	if l.FirstIndex() != 1 || l.LastIndex() != 0 {
		t.Fatalf("first=%d last=%d", l.FirstIndex(), l.LastIndex())
	}
	if _, err := l.Read(1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("got %v", err)
	}
	if countSegFiles(t, dir) != 1 {
		t.Fatalf("files = %d", countSegFiles(t, dir))
	}

	appendN(t, l, 1, 12) // restart from the beginning
	verifyLog(t, l, 1, 12)
	l.Close()

	l = openLog(t, dir, 10)
	defer l.Close()
	verifyLog(t, l, 1, 12)
}

func TestRepeatedTruncateAppend(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 7)
	appendN(t, l, 1, 50)
	// simulate repeated Raft leader changes rewriting the tail
	for round, at := range []uint64{40, 33, 33, 14, 15, 1, 8} {
		if err := l.TruncateFrom(at); err != nil {
			t.Fatalf("round %d truncate %d: %v", round, at, err)
		}
		appendN(t, l, at, 20)
		verifyLog(t, l, 1, at+19)
	}
	last := l.LastIndex()
	l.Close()

	l = openLog(t, dir, 7)
	defer l.Close()
	if l.LastIndex() != last {
		t.Fatalf("last = %d, want %d", l.LastIndex(), last)
	}
	verifyLog(t, l, 1, last)
}

// ---- crash / corruption on Open ----

func TestOpenAcceptsEmptyNewestSegment(t *testing.T) {
	// crash during roll: new file created, nothing written yet
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 20)
	next := l.segPath(21)
	l.Close()

	f, err := os.Create(next)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	l = openLog(t, dir, 10)
	defer l.Close()
	if l.LastIndex() != 20 {
		t.Fatalf("last = %d", l.LastIndex())
	}
	appendN(t, l, 21, 5)
	verifyLog(t, l, 1, 25)
}

func TestOpenTornTailInActiveSegment(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 25)
	lastFile := l.active().path
	l.Close()

	// half-written entry at the end of the last file
	f, err := os.OpenFile(lastFile, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00})
	f.Close()

	l = openLog(t, dir, 10)
	defer l.Close()
	if l.LastIndex() != 25 {
		t.Fatalf("last = %d", l.LastIndex())
	}
	verifyLog(t, l, 1, 25)
	appendN(t, l, 26, 5)
	verifyLog(t, l, 1, 30)
}

func TestOpenMissingMiddleSegment(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 45)
	l.Close()

	if err := os.Remove(l.segPath(21)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{MaxSegmentEntries: 10}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestOpenCorruptSealedSegment(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 45)
	seg := l.segments[1] // base 11, sealed
	pos := seg.offsets[4]
	path := seg.path
	l.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[pos+headerSize] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Must fail loudly, not silently drop entries 16..45.
	if _, err := Open(dir, Options{MaxSegmentEntries: 10}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestOpenBadSegmentFileName(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 5)
	l.Close()

	if err := os.WriteFile(filepath.Join(dir, "garbage"+segmentExt), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("expected error for unparsable segment file name")
	}
}

func TestOpenIgnoresOtherFiles(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir, 10)
	appendN(t, l, 1, 5)
	l.Close()

	os.WriteFile(filepath.Join(dir, "raft-state.bin"), []byte("x"), 0o644)
	os.Mkdir(filepath.Join(dir, "subdir"), 0o755)

	l = openLog(t, dir, 10)
	defer l.Close()
	verifyLog(t, l, 1, 5)
}

// ---- close ----

func TestOperationsAfterClose(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	appendN(t, l, 1, 5)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := l.Append(mkEntry(6)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append: %v", err)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("read: %v", err)
	}
	if _, err := l.Range(1, 3); !errors.Is(err, ErrClosed) {
		t.Fatalf("range: %v", err)
	}
	if err := l.TruncateFrom(3); !errors.Is(err, ErrClosed) {
		t.Fatalf("truncate: %v", err)
	}
	if err := l.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("sync: %v", err)
	}
}

func TestSync(t *testing.T) {
	l := openLog(t, t.TempDir(), 10)
	defer l.Close()
	appendN(t, l, 1, 15)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
}

// ---- concurrency (run with -race) ----

func TestConcurrentAppendAndRead(t *testing.T) {
	l := openLog(t, t.TempDir(), 50)
	defer l.Close()

	const total = 2000
	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := uint64(1); i <= total; i++ {
			if err := l.Append(mkEntry(i)); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				last := l.LastIndex()
				if last == 0 {
					continue
				}
				e, err := l.Read(last)
				if err != nil {
					t.Errorf("read %d: %v", last, err)
					return
				}
				if !entriesEqual(e, mkEntry(last)) {
					t.Errorf("entry %d mismatch", last)
					return
				}
			}
		}()
	}
	wg.Wait()

	if l.LastIndex() != total {
		t.Fatalf("last = %d", l.LastIndex())
	}
	verifyLog(t, l, 1, total)
}
