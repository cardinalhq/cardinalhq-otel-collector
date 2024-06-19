package timebox

import (
	"bytes"
	"io"
)

type MemoryBufferImpl struct {
	buf bytes.Buffer
}

var (
	_ io.Writer     = (*MemoryBufferImpl)(nil)
	_ io.ReadCloser = (*MemoryBufferImpl)(nil)
	_ Buffer        = (*MemoryBufferImpl)(nil)
)

func NewBufferImpl() *MemoryBufferImpl {
	return &MemoryBufferImpl{}
}

func (b *MemoryBufferImpl) Write(p []byte) (n int, err error) {
	return b.buf.Write(p)
}

func (b *MemoryBufferImpl) Read(p []byte) (n int, err error) {
	return b.buf.Read(p)
}

func (b *MemoryBufferImpl) Close() error {
	return nil
}

func (b *MemoryBufferImpl) Cleanup() error {
	return nil
}

func (b *MemoryBufferImpl) CloseWrite() error {
	return nil
}

type MemoryBufferFactory struct{}

var _ BufferFactory = (*MemoryBufferFactory)(nil)

func NewMemoryBufferFactory() *MemoryBufferFactory {
	return &MemoryBufferFactory{}
}

func (bf *MemoryBufferFactory) NewBuffer() (Buffer, error) {
	return NewBufferImpl(), nil
}
