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

type Entry interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

type Timebox[T comparable, E Entry] interface {
	Append(scope T, ts int64, item ...E)
	Closed(scope T, now int64) map[int64][]E
	Items(scope T) map[int64][]E
	Scopes() []T
}

type TimeboxImpl[T comparable, E Entry] struct {
	sync.Mutex
	Interval int64
	Grace    int64
	items    map[T]map[int64]*scopedEntry[E]
}

type mm struct{}

func (*mm) Encode() ([]byte, error) { return nil, nil }
func (*mm) Decode([]byte) error     { return nil }

var _ Timebox[int, *mm] = &TimeboxImpl[int, *mm]{}

type scopedEntry[E Entry] struct {
	ts        int64
	itemCount int
	items     []E
}

func NewTimeboxImpl[T comparable, E Entry](interval int64, grace int64) *TimeboxImpl[T, E] {
	return &TimeboxImpl[T, E]{
		Interval: interval,
		Grace:    grace,
		items:    map[T]map[int64]*scopedEntry[E]{},
	}
}

func (t *TimeboxImpl[T, E]) Append(scope T, ts int64, item ...E) {
	t.Lock()
	defer t.Unlock()
	if _, ok := t.items[scope]; !ok {
		t.items[scope] = map[int64]*scopedEntry[E]{}
	}
	if _, ok := t.items[scope][ts]; !ok {
		t.items[scope][ts] = &scopedEntry[E]{
			ts:    ts,
			items: []E{},
		}
	}
	t.items[scope][ts].items = append(t.items[scope][ts].items, item...)
	t.items[scope][ts].itemCount += len(item)
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

func (t *TimeboxImpl[T, E]) Closed(scope T, now int64) map[int64][]E {
	t.Lock()
	defer t.Unlock()
	ret := map[int64][]E{}
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

func (t *TimeboxImpl[T, E]) Items(scope T) map[int64][]E {
	t.Lock()
	defer t.Unlock()
	ret := map[int64][]E{}
	if _, ok := t.items[scope]; !ok {
		return ret
	}
	for ts, entry := range t.items[scope] {
		ret[ts] = entry.items
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
