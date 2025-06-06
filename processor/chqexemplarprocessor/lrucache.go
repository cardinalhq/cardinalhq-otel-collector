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
	capacity       int
	cache          map[int64]*list.Element
	list           *list.List
	mutex          sync.RWMutex
	expiry         time.Duration
	reportInterval time.Duration
	stop           chan struct{}

	// pending holds entries that must be published on next interval.
	pending []*Entry[T]

	publishCallBack func(items []*Entry[T])
}

type Entry[T any] struct {
	key        int64
	attributes []string
	value      T
	timestamp  time.Time
}

func (e *Entry[T]) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i < len(e.attributes); i += 2 {
		attrs[e.attributes[i]] = e.attributes[i+1]
	}
	return attrs
}

// shouldPublish returns true once an entry has existed for at least half of expiry.
func (e *Entry[T]) shouldPublish(expiry time.Duration) bool {
	return time.Since(e.timestamp) >= expiry/2
}

// NewLRUCache constructs an LRU cache that evicts on capacity and expiry.
// publishCallBack is invoked with a batch of entries to publish at every reportInterval.
func NewLRUCache[T any](
	capacity int,
	expiry time.Duration,
	reportInterval time.Duration,
	publishCallBack func(items []*Entry[T]),
) *LRUCache[T] {
	lru := &LRUCache[T]{
		capacity:        capacity,
		cache:           make(map[int64]*list.Element),
		list:            list.New(),
		expiry:          expiry,
		reportInterval:  reportInterval,
		stop:            make(chan struct{}),
		publishCallBack: publishCallBack,
		pending:         make([]*Entry[T], 0),
	}
	go lru.startBackgroundTasks()
	return lru
}

// startBackgroundTasks launches two tickers: one for cleanup and one for publishing pending.
func (l *LRUCache[T]) startBackgroundTasks() {
	cleanupTicker := time.NewTicker(l.reportInterval)
	publishTicker := time.NewTicker(l.reportInterval)

	for {
		select {
		case <-cleanupTicker.C:
			l.cleanupExpiredEntries()
		case <-publishTicker.C:
			l.flushPending()
		case <-l.stop:
			cleanupTicker.Stop()
			publishTicker.Stop()
			return
		}
	}
}

// cleanupExpiredEntries enqueues half-expired entries and evicts fully-expired ones.
func (l *LRUCache[T]) cleanupExpiredEntries() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()
	for elem := l.list.Back(); elem != nil; {
		prev := elem.Prev()
		entry := elem.Value.(*Entry[T])
		age := now.Sub(entry.timestamp)

		if age >= l.expiry/2 {
			// enqueue for publish if half-expired
			l.pending = append(l.pending, entry)
		}
		if age >= l.expiry {
			// fully expired: remove from cache and list
			l.list.Remove(elem)
			delete(l.cache, entry.key)
		}
		elem = prev
	}
}

// flushPending publishes all pending entries as a single batch.
func (l *LRUCache[T]) flushPending() {
	l.mutex.Lock()
	toPublish := l.pending
	l.pending = make([]*Entry[T], 0)
	l.mutex.Unlock()

	if len(toPublish) > 0 {
		l.publishCallBack(toPublish)
	}
}

// Get returns the value for key if not expired; otherwise it evicts and returns false.
func (l *LRUCache[T]) Get(key int64) (T, bool) {
	l.mutex.RLock()
	elem, found := l.cache[key]
	l.mutex.RUnlock()

	var zero T
	if !found {
		return zero, false
	}

	entry := elem.Value.(*Entry[T])
	if time.Since(entry.timestamp) > l.expiry {
		l.mutex.Lock()
		l.list.Remove(elem)
		delete(l.cache, key)
		l.mutex.Unlock()
		return zero, false
	}
	return entry.value, true
}

// Contains returns true if key exists and is not expired.
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

// Put inserts or updates an entry. If capacity is exceeded, the LRU item is scheduled for publishing.
func (l *LRUCache[T]) Put(key int64, attributes []string, exemplar T) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// If it exists, update timestamp and move to front
	if elem, found := l.cache[key]; found {
		entry := elem.Value.(*Entry[T])
		entry.value = exemplar
		entry.timestamp = time.Now()
		l.list.MoveToFront(elem)
		return
	}

	// Evict LRU if at capacity
	if l.list.Len() >= l.capacity {
		if back := l.list.Back(); back != nil {
			evicted := back.Value.(*Entry[T])
			// enqueue that single evicted entry
			l.pending = append(l.pending, evicted)
			l.list.Remove(back)
			delete(l.cache, evicted.key)
		}
	}

	// Insert new entry at front
	now := time.Now()
	newEntry := &Entry[T]{
		key:        key,
		attributes: attributes,
		value:      exemplar,
		timestamp:  now,
	}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
}

// Remove deletes key from cache, if present.
func (l *LRUCache[T]) Remove(key int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[key]; found {
		l.list.Remove(elem)
		delete(l.cache, key)
	}
}

// Close stops background tasks.
func (l *LRUCache[T]) Close() {
	close(l.stop)
}
