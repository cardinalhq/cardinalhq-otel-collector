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

package chqstatsexporter

import (
	"sync"
	"time"
)

type StatsObject interface {
	Key() uint64
	Matches(StatsObject) bool
	Increment()
}

type StatsCombiner[T StatsObject] struct {
	sync.Mutex
	interval time.Duration
	cutoff   time.Time
	bucket   *map[uint64][]T
}

func (l *StatsCombiner[T]) Record(now time.Time, item T) *map[uint64][]T {
	key := item.Key()
	l.Lock()
	defer l.Unlock()
	list, ok := (*l.bucket)[key]
	if !ok {
		(*l.bucket)[key] = []T{item}
		return l.flush(now)
	}
	for _, existing := range list {
		if existing.Matches(item) {
			existing.Increment()
			return l.flush(now)
		}
	}

	list = append(list, item)
	(*l.bucket)[key] = list
	return l.flush(now)
}

func (l *StatsCombiner[T]) flush(now time.Time) *map[uint64][]T {
	if now.Before(l.cutoff) {
		return nil
	}

	bucketpile := l.bucket
	l.bucket = &map[uint64][]T{}
	l.cutoff = now.Add(l.interval)

	return bucketpile
}

func NewStatsCombiner[T StatsObject](now time.Time, tb TimeboxConfig) *StatsCombiner[T] {
	return &StatsCombiner[T]{
		interval: tb.Interval,
		cutoff:   now.Add(tb.Interval),
		bucket:   &map[uint64][]T{},
	}
}
