// Package storage implements Quorum's durable, append-only log.
//
// Entries are stored back to back in segment files, using this layout
// (big-endian):
//
// | crc (4) | length (4) |  index (8) | term (8) | payload (n) |
// |________Header________|______________Body__________________|
// length is the size of Body
// Checksum over lenth, index, term and payload.
package storage

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	headerSize  = 8  // bodyLen + checkSum
	fixedBody   = 16 // index + term
	maxBodySize = 64 << 20
)

type Entry struct {
	Index   uint64
	Term    uint64
	Payload []byte
}

// encode serializes the entry into it's own disk form: an 8-byte header (crc, length)
// followed by the body (index, term, payload).
func encode(e Entry) []byte {
	bodyLen := fixedBody + len(e.Payload)
	buf := make([]byte, headerSize+bodyLen)

	binary.BigEndian.PutUint32(buf[4:8], uint32(bodyLen))
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	binary.BigEndian.PutUint64(buf[16:24], e.Term)
	copy(buf[24:], e.Payload)

	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:headerSize+bodyLen]))

	return buf
}

// decode parses one complete entry (headr + body)
func decode(buf []byte) (Entry, error) {
	if len(buf) < headerSize {
		return Entry{}, ErrShortBuffer
	}

	bodyLen := int(binary.BigEndian.Uint32(buf[4:8]))
	if bodyLen < fixedBody || bodyLen > maxBodySize {
		return Entry{}, ErrCorrupt
	}

	if len(buf) < headerSize+bodyLen {
		return Entry{}, ErrShortBuffer
	}

	body := buf[headerSize : headerSize+bodyLen]
	if crc32.ChecksumIEEE(buf[4:headerSize+bodyLen]) != binary.BigEndian.Uint32(buf[0:4]) {
		return Entry{}, ErrCorrupt
	}

	payload := make([]byte, bodyLen-fixedBody)
	copy(payload, body[fixedBody:])

	return Entry{
		Index:   binary.BigEndian.Uint64(body[0:8]),
		Term:    binary.BigEndian.Uint64(body[8:16]),
		Payload: payload,
	}, nil
}
