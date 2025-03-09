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
	"hash/fnv"
	"strings"
	"sync"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
)

var serviceNameKey = string(semconv.ServiceNameKey)
var clusterNameKey = string(semconv.K8SClusterNameKey)
var namespaceNameKey = string(semconv.K8SNamespaceNameKey)
var metricNameKey = "metric.name"
var metricTypeKey = "metric.type"

type LRUCache struct {
	capacity        int
	cache           map[int64]*list.Element
	list            *list.List
	mutex           sync.RWMutex
	expiry          time.Duration
	reportInterval  time.Duration
	stopCleanup     chan struct{}
	publishCallBack func(toPublish []*Entry)
}

type Entry struct {
	fingerprint     int64
	keys            []string
	value           any
	timestamp       time.Time
	lastPublishTime time.Time
}

func (e *Entry) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i < len(e.keys); i += 2 {
		attrs[e.keys[i]] = e.keys[i+1]
	}
	return attrs
}

func (e *Entry) shouldPublish(expiry time.Duration) bool {
	now := time.Now()
	return now.Sub(e.lastPublishTime) > expiry/2
}

func NewLRUCache(capacity int, expiry time.Duration, reportInterval time.Duration, publishCallBack func(expiredItems []*Entry)) *LRUCache {
	lru := &LRUCache{
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

func (l *LRUCache) startCleanup() {
	for {
		select {
		case <-time.NewTicker(l.reportInterval).C:
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
	itemsToPublish := make([]*Entry, 0)
	for e := l.list.Back(); e != nil; {
		entry := e.Value.(*Entry)
		if entry.shouldPublish(l.expiry) {
			itemsToPublish = append(itemsToPublish, entry)
			entry.lastPublishTime = now
		}
		if now.Sub(entry.timestamp) > l.expiry {
			prev := e.Prev()
			l.list.Remove(e)
			delete(l.cache, entry.fingerprint)
			e = prev
		} else {
			e = e.Prev()
		}
	}
	if len(itemsToPublish) > 0 {
		l.publishCallBack(itemsToPublish)
	}
}

func (l *LRUCache) Get(key int64) (any, bool) {
	l.mutex.RLock()
	elem, found := l.cache[key]
	l.mutex.RUnlock()

	if !found {
		return nil, false
	}

	entry := elem.Value.(*Entry)
	if time.Since(entry.timestamp) > l.expiry {
		l.mutex.Lock()
		defer l.mutex.Unlock()
		l.list.Remove(elem)
		delete(l.cache, key)
		return nil, false
	}

	return entry.value, true
}

func hashString(s []string) int64 {
	h := fnv.New64a()
	h.Write([]byte(strings.Join(s, ":")))
	return int64(h.Sum64())
}

func (l *LRUCache) Contains(key int64) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	elem, found := l.cache[key]
	if !found {
		return false
	}

	entry := elem.Value.(*Entry)
	return time.Since(entry.timestamp) <= l.expiry
}

// Put adds a value to the cache or updates it if it already exists.
func (l *LRUCache) Put(fingerprint int64, keys []string, exemplar any) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, found := l.cache[fingerprint]; found {
		entry := elem.Value.(*Entry)
		entry.value = exemplar
		entry.timestamp = time.Now()
		l.list.MoveToFront(elem)
		return
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			entry := back.Value.(*Entry)
			l.publishCallBack([]*Entry{entry})
			l.list.Remove(back)
			delete(l.cache, entry.fingerprint)
		}
	}

	now := time.Now()
	newEntry := &Entry{
		fingerprint:     fingerprint,
		keys:            keys,
		value:           exemplar,
		timestamp:       now,
		lastPublishTime: now}
	elem := l.list.PushFront(newEntry)
	l.cache[fingerprint] = elem
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
