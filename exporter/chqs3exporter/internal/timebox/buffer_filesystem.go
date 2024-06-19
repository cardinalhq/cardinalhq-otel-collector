// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	_, err := bf.file.Seek(0, 0)
	return err
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
