package timebox

import (
	"os"
)

type BufferFilesystem struct {
	file    *os.File
	writing bool
}

var _ Buffer = (*BufferFilesystem)(nil)

func NewBufferFilesystem(file *os.File) (*BufferFilesystem, error) {
	return &BufferFilesystem{
		file:    file,
		writing: true,
	}, nil
}

func (bf *BufferFilesystem) Write(p []byte) (n int, err error) {
	if !bf.writing {
		return 0, os.ErrClosed
	}
	return bf.file.Write(p)
}

func (bf *BufferFilesystem) CloseWrite() error {
	bf.writing = false
	bf.file.Seek(0, 0)
	return nil
}

func (bf *BufferFilesystem) Read(p []byte) (n int, err error) {
	if bf.writing {
		return 0, os.ErrClosed
	}
	return bf.file.Read(p)
}

func (bf *BufferFilesystem) Close() error {
	defer os.Remove(bf.file.Name())
	return bf.file.Close()
}

func (bf *BufferFilesystem) Cleanup() error {
	return os.Remove(bf.file.Name())
}

type BufferFilesystemFactory struct {
	dir string
}

var _ BufferFactory = (*BufferFilesystemFactory)(nil)

func NewBufferFilesystemFactory(dir string) *BufferFilesystemFactory {
	return &BufferFilesystemFactory{
		dir: dir,
	}
}

func (bf *BufferFilesystemFactory) NewBuffer() (Buffer, error) {
	file, err := os.CreateTemp(bf.dir, "buffer*")
	if err != nil {
		return nil, err
	}
	return NewBufferFilesystem(file)
}
