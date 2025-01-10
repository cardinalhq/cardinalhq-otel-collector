// Copyright 2024-2025 CardinalHQ, Inc
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

package boxer

import (
	"bytes"
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/exp/maps"
)

type MemoryItem struct {
	buffer   *bytes.Buffer
	expected int
}

type MemoryBuffer struct {
	sync.Mutex
	records  map[int64]map[string]*MemoryItem
	shutdown bool
}

var (
	_ Buffer = (*MemoryBuffer)(nil)
)

func NewMemoryBuffer() *MemoryBuffer {
	return &MemoryBuffer{
		records: make(map[int64]map[string]*MemoryItem),
	}
}

func (b *MemoryBuffer) Write(data *BufferRecord) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	if _, ok := b.records[data.Interval]; !ok {
		b.records[data.Interval] = make(map[string]*MemoryItem)
	}
	if _, ok := b.records[data.Interval][data.Scope]; !ok {
		b.records[data.Interval][data.Scope] = &MemoryItem{
			buffer: &bytes.Buffer{},
		}
	}
	item := b.records[data.Interval][data.Scope]
	item.expected++
	return encodeToFile(item.buffer, data)
}

func (b *MemoryBuffer) GetScopes(interval int64) (scopes []string, err error) {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return nil, ErrShutdown
	}

	return maps.Keys(b.records[interval]), nil
}

func (b *MemoryBuffer) GetIntervals() (intervals []int64, err error) {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return nil, ErrShutdown
	}

	return maps.Keys(b.records), nil
}

func (b *MemoryBuffer) ForEach(interval int64, scope string, fn ForEachFunc) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	if _, ok := b.records[interval]; !ok {
		return NoSuchIntervalError
	}
	if _, ok := b.records[interval][scope]; !ok {
		return NoSuchScopeError
	}
	item := b.records[interval][scope]
	return iterate(bytes.NewReader(item.buffer.Bytes()), item.expected, fn)
}

func (b *MemoryBuffer) CloseIntervalScope(interval int64, scope string) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	return b.unlockedCloseIntervalScope(interval, scope)
}

func (b *MemoryBuffer) unlockedCloseIntervalScope(interval int64, scope string) error {
	delete(b.records[interval], scope)
	if len(b.records[interval]) == 0 {
		delete(b.records, interval)
	}
	return nil
}

func (b *MemoryBuffer) CloseInterval(interval int64) error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	for interval := range b.records {
		for scope := range b.records[interval] {
			err := b.unlockedCloseIntervalScope(interval, scope)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *MemoryBuffer) Shutdown() error {
	b.Lock()
	defer b.Unlock()
	b.shutdown = true
	b.records = nil
	return nil
}

func (b *MemoryBuffer) Wipe() error {
	b.Lock()
	defer b.Unlock()
	if b.shutdown {
		return ErrShutdown
	}

	var errs *multierror.Error
	for interval := range b.records {
		for scope := range b.records[interval] {
			errs = multierror.Append(errs, b.unlockedCloseIntervalScope(interval, scope))
		}
	}
	b.records = make(map[int64]map[string]*MemoryItem)
	return errs.ErrorOrNil()
}
