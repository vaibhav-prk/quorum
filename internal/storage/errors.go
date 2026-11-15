package storage

import (
	"errors"
)

var (
	ErrShortBuffer = errors.New("storage: short buffer")
	ErrCorrupt     = errors.New("storage: corrupt entry")
	ErrBadIndex    = errors.New("storage: entry index is not the next in sequence")
	ErrOutOfRange  = errors.New("storage: index out of range")
	ErrTooLarge    = errors.New("storage: entry payload exceeds maximum size")
	ErrClosed      = errors.New("storage: log is closed")
)
