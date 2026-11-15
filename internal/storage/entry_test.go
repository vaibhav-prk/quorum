package storage

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func entriesEqual(a, b Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Payload, b.Payload)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := map[string]Entry{
		"empty payload": {Index: 1, Term: 1, Payload: []byte{}},
		"nil payload":   {Index: 2, Term: 1, Payload: nil},
		"one byte":      {Index: 3, Term: 2, Payload: []byte{0x00}},
		"text":          {Index: 4, Term: 2, Payload: []byte("hello quorum")},
		"zero values":   {Index: 0, Term: 0, Payload: []byte("x")},
		"max values":    {Index: math.MaxUint64, Term: math.MaxUint64, Payload: []byte("max")},
		"binary bytes":  {Index: 5, Term: 3, Payload: []byte{0, 0xFF, 0, 0xFF, 0x80}},
		"1 MiB":         {Index: 6, Term: 3, Payload: bytes.Repeat([]byte{0xAB}, 1<<20)},
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := decode(encode(e))
			if err != nil {
				t.Fatal(err)
			}
			if !entriesEqual(got, e) {
				t.Fatalf("got %+v, want %+v", got, e)
			}
		})
	}
}

func TestEncodedLength(t *testing.T) {
	for _, n := range []int{0, 1, 100, 4096} {
		buf := encode(Entry{Index: 1, Term: 1, Payload: make([]byte, n)})
		if want := headerSize + fixedBody + n; len(buf) != want {
			t.Fatalf("payload %d: len = %d, want %d", n, len(buf), want)
		}
	}
}

// Pins the on-disk format. If this breaks, you changed the file format.
func TestEncodeLayout(t *testing.T) {
	buf := encode(Entry{Index: 0x0102030405060708, Term: 0x1112131415161718, Payload: []byte("ab")})

	if got := binary.BigEndian.Uint32(buf[4:8]); got != fixedBody+2 {
		t.Fatalf("length field = %d", got)
	}
	if got := binary.BigEndian.Uint64(buf[8:16]); got != 0x0102030405060708 {
		t.Fatalf("index field = %x", got)
	}
	if got := binary.BigEndian.Uint64(buf[16:24]); got != 0x1112131415161718 {
		t.Fatalf("term field = %x", got)
	}
	if string(buf[24:]) != "ab" {
		t.Fatalf("payload = %q", buf[24:])
	}
	// big-endian: most significant byte first
	if buf[8] != 0x01 || buf[15] != 0x08 {
		t.Fatal("index is not big-endian")
	}
}

// Every single-byte flip anywhere in the entry must be detected.
func TestDecodeDetectsAnyByteFlip(t *testing.T) {
	orig := encode(Entry{Index: 7, Term: 3, Payload: []byte("some payload bytes")})
	for i := range orig {
		buf := append([]byte(nil), orig...)
		buf[i] ^= 0xFF
		if _, err := decode(buf); err == nil {
			t.Fatalf("flip at byte %d not detected", i)
		}
	}
}

func TestDecodeDetectsSingleBitFlip(t *testing.T) {
	orig := encode(Entry{Index: 7, Term: 3, Payload: []byte("some payload bytes")})
	for i := range orig {
		for bit := 0; bit < 8; bit++ {
			buf := append([]byte(nil), orig...)
			buf[i] ^= 1 << bit
			if _, err := decode(buf); err == nil {
				t.Fatalf("bit flip at byte %d bit %d not detected", i, bit)
			}
		}
	}
}

// Every proper prefix of a valid entry must fail as a short buffer.
func TestDecodeTruncated(t *testing.T) {
	orig := encode(Entry{Index: 1, Term: 1, Payload: []byte("truncate me")})
	for n := 0; n < len(orig); n++ {
		if _, err := decode(orig[:n]); err != ErrShortBuffer {
			t.Fatalf("len %d: got %v, want ErrShortBuffer", n, err)
		}
	}
}

func TestDecodeEmptyAndNil(t *testing.T) {
	if _, err := decode(nil); err != ErrShortBuffer {
		t.Fatalf("nil: got %v", err)
	}
	if _, err := decode([]byte{}); err != ErrShortBuffer {
		t.Fatalf("empty: got %v", err)
	}
}

func TestDecodeLengthBelowFixedBody(t *testing.T) {
	buf := make([]byte, headerSize+fixedBody)
	for _, l := range []uint32{0, 1, fixedBody - 1} {
		binary.BigEndian.PutUint32(buf[4:8], l)
		if _, err := decode(buf); err != ErrCorrupt {
			t.Fatalf("length %d: got %v, want ErrCorrupt", l, err)
		}
	}
}

func TestDecodeHugeLength(t *testing.T) {
	buf := encode(Entry{Index: 1, Term: 1, Payload: []byte("x")})
	binary.BigEndian.PutUint32(buf[4:8], 0xFFFFFFFF)
	if _, err := decode(buf); err == nil {
		t.Fatal("expected error for absurd length")
	}
}

func TestDecodeBadChecksumField(t *testing.T) {
	buf := encode(Entry{Index: 1, Term: 1, Payload: []byte("x")})
	buf[0] ^= 0x01
	if _, err := decode(buf); err != ErrCorrupt {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

// Segment.recover/Read pass exactly one entry; extra bytes after it are ignored.
func TestDecodeIgnoresTrailingBytes(t *testing.T) {
	e := Entry{Index: 9, Term: 4, Payload: []byte("data")}
	buf := append(encode(e), 0xDE, 0xAD, 0xBE, 0xEF)
	got, err := decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !entriesEqual(got, e) {
		t.Fatalf("got %+v", got)
	}
}

// Decoded payload must not alias the input buffer.
func TestDecodePayloadIsCopy(t *testing.T) {
	buf := encode(Entry{Index: 1, Term: 1, Payload: []byte("original")})
	got, err := decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := range buf {
		buf[i] = 0
	}
	if string(got.Payload) != "original" {
		t.Fatalf("payload aliased input: %q", got.Payload)
	}
}

// Encoded bytes must not alias the caller's payload.
func TestEncodeDoesNotAlias(t *testing.T) {
	payload := []byte("original")
	buf := encode(Entry{Index: 1, Term: 1, Payload: payload})
	payload[0] = 'X'
	got, err := decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "original" {
		t.Fatalf("got %q", got.Payload)
	}
}

func TestEncodeDeterministic(t *testing.T) {
	e := Entry{Index: 5, Term: 2, Payload: []byte("same")}
	if !bytes.Equal(encode(e), encode(e)) {
		t.Fatal("encode is not deterministic")
	}
}

func TestDifferentEntriesDifferentChecksums(t *testing.T) {
	a := encode(Entry{Index: 1, Term: 1, Payload: []byte("a")})
	b := encode(Entry{Index: 1, Term: 1, Payload: []byte("b")})
	if bytes.Equal(a[0:4], b[0:4]) {
		t.Fatal("different payloads produced the same crc")
	}
	c := encode(Entry{Index: 2, Term: 1, Payload: []byte("a")})
	if bytes.Equal(a[0:4], c[0:4]) {
		t.Fatal("different index produced the same crc")
	}
}
