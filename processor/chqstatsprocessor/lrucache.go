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

package chqstatsprocessor

import (
	"container/list"
	"sync"
	"time"
)

type LRUCache struct {
	capacity    int
	cache       map[int64]*list.Element
	list        *list.List
	mutex       sync.RWMutex
	expiry      time.Duration
	stopCleanup chan struct{}
}

type Entry struct {
	key       int64
	value     interface{}
	timestamp time.Time
}

func NewLRUCache(capacity int, expiry time.Duration) *LRUCache {
	lru := &LRUCache{
		capacity:    capacity,
		cache:       make(map[int64]*list.Element),
		list:        list.New(),
		expiry:      expiry,
		stopCleanup: make(chan struct{}),
	}
	go lru.startCleanup()
	return lru
}

func (l *LRUCache) startCleanup() {
	ticker := time.NewTicker(l.expiry / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.cleanupExpiredEntries()
		case <-l.stopCleanup:
			return
		}
	}
}

func (l *LRUCache) cleanupExpiredEntries() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()
	for e := l.list.Back(); e != nil; {
		entry := e.Value.(*Entry)
		if now.Sub(entry.timestamp) > l.expiry {
			prev := e.Prev()
			l.list.Remove(e)
			delete(l.cache, entry.key)
			e = prev
		} else {
			e = e.Prev()
		}
	}
}

func (l *LRUCache) Contains(key int64) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	elem, found := l.cache[key]
	if !found {
		return false
	}

	entry := elem.Value.(*Entry)
	if time.Since(entry.timestamp) > l.expiry {
		return false
	}

	return true
}

func (l *LRUCache) Get(key int64) (interface{}, bool) {
	l.mutex.RLock()
	elem, found := l.cache[key]
	l.mutex.RUnlock()

	if found {
		l.mutex.Lock()
		defer l.mutex.Unlock()

		entry := elem.Value.(*Entry)
		if time.Since(entry.timestamp) > l.expiry {
			l.list.Remove(elem)
			delete(l.cache, key)
			return nil, false
		}

		entry.timestamp = time.Now()
		l.list.MoveToFront(elem)
		return entry.value, true
	}
	return nil, false
}

// Put adds a value to the cache or updates it if it already exists.
func (l *LRUCache) Put(key int64, value interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		entry := elem.Value.(*Entry)
		entry.value = value
		entry.timestamp = time.Now()
		l.list.MoveToFront(elem)
		return
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			l.list.Remove(back)
			delete(l.cache, back.Value.(*Entry).key)
		}
	}

	newEntry := &Entry{key: key, value: value, timestamp: time.Now()}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
}

func (l *LRUCache) Remove(key int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		l.list.Remove(elem)
		delete(l.cache, key)
	}
}

func (l *LRUCache) Close() {
	close(l.stopCleanup)
}
