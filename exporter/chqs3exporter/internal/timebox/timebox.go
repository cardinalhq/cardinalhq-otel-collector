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

import "sync"

type Timebox[T comparable] interface {
	Append(scope T, ts int64, item ...Entry)
	Closed(scope T, now int64) map[int64][]Entry
	Items(scope T) map[int64][]Entry
	Scopes() []T
}

type TimeboxImpl[T comparable] struct {
	sync.Mutex
	Interval int64
	Grace    int64
	items    map[T]map[int64]*scopedEntry
}

var _ Timebox[int] = &TimeboxImpl[int]{}

type scopedEntry struct {
	ts        int64
	itemCount int
	items     []Entry
}

type Entry interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

func NewTimeboxImpl[T comparable](interval int64, grace int64) *TimeboxImpl[T] {
	return &TimeboxImpl[T]{
		Interval: interval,
		Grace:    grace,
		items:    map[T]map[int64]*scopedEntry{},
	}
}

func (t *TimeboxImpl[T]) Append(scope T, ts int64, item ...Entry) {
	t.Lock()
	defer t.Unlock()
	if _, ok := t.items[scope]; !ok {
		t.items[scope] = map[int64]*scopedEntry{}
	}
	if _, ok := t.items[scope][ts]; !ok {
		t.items[scope][ts] = &scopedEntry{
			ts:    ts,
			items: []Entry{},
		}
	}
	t.items[scope][ts].items = append(t.items[scope][ts].items, item...)
	t.items[scope][ts].itemCount += len(item)
}

func (t *TimeboxImpl[T]) ItemCount(scope T, ts int64) int {
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

func (t *TimeboxImpl[T]) Closed(scope T, now int64) map[int64][]Entry {
	t.Lock()
	defer t.Unlock()
	ret := map[int64][]Entry{}
	if _, ok := t.items[scope]; !ok {
		return ret
	}
	for ts, entry := range t.items[scope] {
		if closed(now, entry.ts, t.Interval, t.Grace) {
			ret[ts] = entry.items
			delete(t.items[scope], ts)
		}
	}
	return ret
}

func closed(now, tbstart, interval, grace int64) bool {
	return now-tbstart >= interval+grace
}

func (t *TimeboxImpl[T]) Items(scope T) map[int64][]Entry {
	t.Lock()
	defer t.Unlock()
	ret := map[int64][]Entry{}
	if _, ok := t.items[scope]; !ok {
		return ret
	}
	for ts, entry := range t.items[scope] {
		ret[ts] = entry.items
	}
	return ret
}

func (t *TimeboxImpl[T]) Scopes() []T {
	t.Lock()
	defer t.Unlock()
	ret := []T{}
	for scope := range t.items {
		ret = append(ret, scope)
	}
	return ret
}
