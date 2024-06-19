package timebox

import (
	"io"
)

type Buffer interface {
	io.Writer
	io.ReadCloser
	Cleanup() error
	CloseWrite() error
}

type BufferFactory interface {
	NewBuffer() (Buffer, error)
}
