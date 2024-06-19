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
	"sync"
)

type Entry interface {
	Encode() ([]byte, error)
	New([]byte) (Entry, error)
}

type Timebox[T comparable, E Entry] interface {
	Append(scope T, ts int64, item ...E) error
	Closed(scope T, now int64, generator E) map[int64][]E
	Items(scope T, generator E) map[int64][]E
	Scopes() []T
	TooOld(ts int64, now int64) bool
}

type TimeboxImpl[T comparable, E Entry] struct {
	sync.Mutex
	Interval      int64
	IntervalCount int64
	Grace         int64
	items         map[T]map[int64]*scopedEntry[E]
	BufferFactory BufferFactory
}

type mm struct{}

func (*mm) Encode() ([]byte, error)   { return nil, nil }
func (*mm) New([]byte) (Entry, error) { return &mm{}, nil }

var _ Timebox[int, *mm] = &TimeboxImpl[int, *mm]{}

type scopedEntry[E Entry] struct {
	ts        int64
	itemCount int
	buffer    Buffer
	generator E
}

func NewTimeboxImpl[T comparable, E Entry](interval int64, intervalCount int64, grace int64) *TimeboxImpl[T, E] {
	return &TimeboxImpl[T, E]{
		Interval:      interval,
		IntervalCount: intervalCount,
		Grace:         grace,
		items:         map[T]map[int64]*scopedEntry[E]{},
		BufferFactory: NewMemoryBufferFactory(),
	}
}

func (t *TimeboxImpl[T, E]) TooOld(ts int64, now int64) bool {
	return now-ts >= t.Interval*t.IntervalCount+t.Grace
}

func (t *TimeboxImpl[T, E]) Append(scope T, ts int64, newItems ...E) error {
	t.Lock()
	defer t.Unlock()
	if _, ok := t.items[scope]; !ok {
		t.items[scope] = map[int64]*scopedEntry[E]{}
	}
	if _, ok := t.items[scope][ts]; !ok {
		buffer, err := t.BufferFactory.NewBuffer()
		if err != nil {
			return err
		}
		t.items[scope][ts] = &scopedEntry[E]{
			ts:     ts,
			buffer: buffer,
		}
	}
	for _, i := range newItems {
		b, err := i.Encode()
		if err != nil {
			return err
		}
		length := int32(len(b))
		lengthBytes := []byte{
			byte(length >> 24),
			byte(length >> 16),
			byte(length >> 8),
			byte(length),
		}
		_, err = t.items[scope][ts].buffer.Write(lengthBytes)
		if err != nil {
			return err
		}
		_, err = t.items[scope][ts].buffer.Write(b)
		if err != nil {
			return err
		}
	}
	t.items[scope][ts].itemCount += len(newItems)
	return nil
}

func (t *TimeboxImpl[T, E]) ItemCount(scope T, ts int64) int {
	t.Lock()
	defer t.Unlock()
	if _, ok := t.items[scope]; !ok {
		return 0
	}
	if _, ok := t.items[scope][ts]; !ok {
		return 0
	}
	return t.items[scope][ts].itemCount
}

func (se *scopedEntry[E]) decode(generator E) ([]E, error) {
	items := make([]E, se.itemCount, se.itemCount)
	for i := 0; i < se.itemCount; i++ {
		lengthBytes := make([]byte, 4)
		_, err := se.buffer.Read(lengthBytes)
		length := int32(lengthBytes[0])<<24 | int32(lengthBytes[1])<<16 | int32(lengthBytes[2])<<8 | int32(lengthBytes[3])
		b := make([]byte, length)
		_, err = se.buffer.Read(b)
		newItem, err := generator.New(b)
		if err != nil {
			return nil, err
		}
		items[i] = newItem.(E)
	}
	return items, nil
}

func (t *TimeboxImpl[T, E]) Closed(scope T, now int64, generator E) map[int64][]E {
	t.Lock()
	defer t.Unlock()
	ret := map[int64][]E{}
	if _, ok := t.items[scope]; !ok {
		return ret
	}
	for ts, entry := range t.items[scope] {
		if closed(now, entry.ts, t.Interval, t.IntervalCount, t.Grace) {
			items, err := entry.decode(generator)
			if err != nil {
				continue
			}
			ret[ts] = items
			delete(t.items[scope], ts)
		}
	}
	return ret
}

func closed(now, tbstart, interval, nIntervals, grace int64) bool {
	return now-tbstart >= interval*nIntervals+grace
}

func (t *TimeboxImpl[T, E]) Items(scope T, generator E) map[int64][]E {
	t.Lock()
	defer t.Unlock()
	ret := map[int64][]E{}
	if _, ok := t.items[scope]; !ok {
		return ret
	}
	for ts, entry := range t.items[scope] {
		items, err := entry.decode(generator)
		if err != nil {
			continue
		}
		ret[ts] = items
	}
	return ret
}

func (t *TimeboxImpl[T, E]) Scopes() []T {
	t.Lock()
	defer t.Unlock()
	ret := []T{}
	for scope := range t.items {
		ret = append(ret, scope)
	}
	return ret
}
