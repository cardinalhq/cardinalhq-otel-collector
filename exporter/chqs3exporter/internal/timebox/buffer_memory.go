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
