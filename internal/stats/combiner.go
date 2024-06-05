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

package stats

import (
	"sync"
	"time"
)

type StatsObject interface {
	Key() uint64
	Matches(other StatsObject) bool
	Increment(key string, count int, size int64) error
	Initialize() error
}

type StatsCombiner[T StatsObject] struct {
	sync.Mutex
	interval time.Duration
	cutoff   time.Time
	bucket   *map[uint64][]T
}

func (l *StatsCombiner[T]) Record(now time.Time, item T, incKey string, count int, logSize int64) (*map[uint64][]T, error) {
	key := item.Key()
	l.Lock()
	defer l.Unlock()
	list, ok := (*l.bucket)[key]
	if !ok {
		if err := item.Initialize(); err != nil {
			return nil, err
		}
		(*l.bucket)[key] = []T{item}
		return l.flush(now), nil
	}
	for _, existing := range list {
		if existing.Matches(item) {
			if err := existing.Increment(incKey, count, logSize); err != nil {
				return nil, err
			}
			return l.flush(now), nil
		}
	}

	if err := item.Initialize(); err != nil {
		return nil, err
	}
	(*l.bucket)[key] = append((*l.bucket)[key], item)
	return l.flush(now), nil
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

func NewStatsCombiner[T StatsObject](now time.Time, interval time.Duration) *StatsCombiner[T] {
	return &StatsCombiner[T]{
		interval: interval,
		cutoff:   now.Add(interval),
		bucket:   &map[uint64][]T{},
	}
}
