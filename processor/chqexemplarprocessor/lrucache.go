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

package chqexemplarprocessor

import (
	"container/list"
	"sync"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
)

const (
	serviceNameKey   = string(semconv.ServiceNameKey)
	clusterNameKey   = string(semconv.K8SClusterNameKey)
	namespaceNameKey = string(semconv.K8SNamespaceNameKey)
	metricNameKey    = "metric.name"
	metricTypeKey    = "metric.type"
)

type LRUCache[T any] struct {
	capacity        int
	cache           map[int64]*list.Element
	list            *list.List
	mutex           sync.RWMutex
	expiry          time.Duration
	reportInterval  time.Duration
	stopCleanup     chan struct{}
	publishCallBack func(toPublish []*Entry[T])
}

type Entry[T any] struct {
	key             int64
	attributes      []string
	value           T
	timestamp       time.Time
	lastPublishTime time.Time
}

func (e *Entry[T]) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i < len(e.attributes); i += 2 {
		attrs[e.attributes[i]] = e.attributes[i+1]
	}
	return attrs
}

func (e *Entry[T]) shouldPublish(expiry time.Duration) bool {
	now := time.Now()
	return now.Sub(e.lastPublishTime) > expiry/2
}

func NewLRUCache[T any](capacity int, expiry time.Duration, reportInterval time.Duration, publishCallBack func(expiredItems []*Entry[T])) *LRUCache[T] {
	lru := &LRUCache[T]{
		capacity:        capacity,
		cache:           make(map[int64]*list.Element),
		list:            list.New(),
		reportInterval:  reportInterval,
		expiry:          expiry,
		stopCleanup:     make(chan struct{}),
		publishCallBack: publishCallBack,
	}
	go lru.startCleanup()
	return lru
}

func (l *LRUCache[T]) startCleanup() {
	for {
		select {
		case <-time.NewTicker(l.reportInterval).C:
			l.cleanupExpiredEntries()
		case <-l.stopCleanup:
			return
		}
	}
}

func (l *LRUCache[T]) cleanupExpiredEntries() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()
	itemsToPublish := make([]*Entry[T], 0)
	for e := l.list.Back(); e != nil; {
		entry := e.Value.(*Entry[T])
		if entry.shouldPublish(l.expiry) {
			itemsToPublish = append(itemsToPublish, entry)
			entry.lastPublishTime = now
		}
		if now.Sub(entry.timestamp) > l.expiry {
			prev := e.Prev()
			l.list.Remove(e)
			delete(l.cache, entry.key)
			e = prev
		} else {
			e = e.Prev()
		}
	}
	if len(itemsToPublish) > 0 {
		l.publishCallBack(itemsToPublish)
	}
}

func (l *LRUCache[T]) Get(key int64) (any, bool) {
	l.mutex.RLock()
	elem, found := l.cache[key]
	l.mutex.RUnlock()

	if !found {
		return nil, false
	}

	entry := elem.Value.(*Entry[T])
	if time.Since(entry.timestamp) > l.expiry {
		l.mutex.Lock()
		l.list.Remove(elem)
		delete(l.cache, key)
		l.mutex.Unlock()
		return nil, false
	}

	return entry.value, true
}

func (l *LRUCache[T]) Contains(key int64) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	elem, found := l.cache[key]
	if !found {
		return false
	}

	entry := elem.Value.(*Entry[T])
	return time.Since(entry.timestamp) <= l.expiry
}

// Put adds a value to the cache or updates it if it already exists.
func (l *LRUCache[T]) Put(key int64, keys []string, exemplar T) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		entry := elem.Value.(*Entry[T])
		entry.value = exemplar
		entry.timestamp = time.Now()
		l.list.MoveToFront(elem)
		return
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			entry := back.Value.(*Entry[T])
			l.publishCallBack([]*Entry[T]{entry})
			l.list.Remove(back)
			delete(l.cache, entry.key)
		}
	}

	now := time.Now()
	newEntry := &Entry[T]{
		key:             key,
		attributes:      keys,
		value:           exemplar,
		timestamp:       now,
		lastPublishTime: now}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
}

func (l *LRUCache[T]) Remove(key int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		l.list.Remove(elem)
		delete(l.cache, key)
	}
}

func (l *LRUCache[T]) Close() {
	close(l.stopCleanup)
}
