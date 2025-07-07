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

package chqentitygraphexporter

import (
	"container/list"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type SpanEntry struct {
	spanId            string
	key               int64
	fingerprint       int64
	parentFingerprint int64
	attributes        []string
	exemplar          ptrace.Traces
	timestamp         time.Time
	lastPublishTime   time.Time
}

type SpanLRUCache struct {
	capacity       int
	expiry         time.Duration
	reportInterval time.Duration

	cache map[int64]*list.Element
	list  *list.List
	mutex sync.RWMutex

	// maps and queues for parent-child resolution
	spanIdToFingerprint map[string]int64
	waiting             map[string][]*SpanEntry

	pending         []*SpanEntry
	stopCleanup     chan struct{}
	publishCallback func([]*SpanEntry)
}

// NewSpanLRUCache creates a new cache with parent resolution support
func NewSpanLRUCache(capacity int, expiry, reportInterval time.Duration, publishCallback func([]*SpanEntry)) *SpanLRUCache {
	c := &SpanLRUCache{
		capacity:            capacity,
		expiry:              expiry,
		reportInterval:      reportInterval,
		cache:               make(map[int64]*list.Element),
		list:                list.New(),
		stopCleanup:         make(chan struct{}),
		publishCallback:     publishCallback,
		pending:             make([]*SpanEntry, 0),
		spanIdToFingerprint: make(map[string]int64),
		waiting:             make(map[string][]*SpanEntry),
	}
	go c.startCleanup()
	return c
}

func (e *SpanEntry) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i < len(e.attributes); i += 2 {
		attrs[e.attributes[i]] = e.attributes[i+1]
	}
	attrs["parent.fingerprint"] = strconv.FormatInt(e.parentFingerprint, 10)
	return attrs
}

func (c *SpanLRUCache) startCleanup() {
	ticker := time.NewTicker(c.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// evictSpan removes one entry from all of our maps/list/cache
func (c *SpanLRUCache) evictSpan(e *list.Element) {
	entry := e.Value.(*SpanEntry)

	// remove helper maps
	delete(c.spanIdToFingerprint, entry.spanId)
	delete(c.waiting, entry.spanId)

	// remove from LRU
	c.list.Remove(e)
	delete(c.cache, entry.key)
}

func (c *SpanLRUCache) cleanupExpired() {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for e := c.list.Back(); e != nil; {
		prev := e.Prev()
		entry := e.Value.(*SpanEntry)

		if now.Sub(entry.timestamp) > c.expiry {
			c.evictSpan(e)
		}
		e = prev
	}

	if len(c.pending) > 0 {
		c.publishCallback(c.pending)
		c.pending = c.pending[:0]
	}
}

func (c *SpanLRUCache) Put(
	key int64, spanID, parentSpanID string,
	fingerprint int64, exemplar ptrace.Traces, attributes []string,
) {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.list.Len() >= c.capacity {
		if back := c.list.Back(); back != nil {
			if time.Since(back.Value.(*SpanEntry).lastPublishTime) >= c.expiry {
				c.pending = append(c.pending, back.Value.(*SpanEntry))
				back.Value.(*SpanEntry).lastPublishTime = now
			}
			c.evictSpan(back)
		}
	}

	entry := &SpanEntry{
		spanId:          spanID,
		key:             key,
		fingerprint:     fingerprint,
		attributes:      attributes,
		exemplar:        exemplar,
		timestamp:       now,
		lastPublishTime: now,
	}
	elem := c.list.PushFront(entry)
	c.cache[key] = elem
	c.pending = append(c.pending, entry)

	c.spanIdToFingerprint[spanID] = fingerprint
	c.resolveWaiting(spanID)

	if parentFp, found := c.spanIdToFingerprint[parentSpanID]; found {
		entry.parentFingerprint = parentFp
	} else if parentSpanID != "" {
		c.waiting[parentSpanID] = append(c.waiting[parentSpanID], entry)
	}
}

// resolveWaiting stamps any children waiting on spanID
func (c *SpanLRUCache) resolveWaiting(spanID string) {
	if children, ok := c.waiting[spanID]; ok {
		pf := c.spanIdToFingerprint[spanID]
		for _, child := range children {
			child.parentFingerprint = pf
		}
		delete(c.waiting, spanID)
	}
}

func (c *SpanLRUCache) Contains(spanID string, key int64) bool {
	c.mutex.RLock()
	_, inCache := c.cache[key]
	waitingList := c.waiting[spanID]
	c.mutex.RUnlock()

	if !inCache {
		return false
	}

	if len(waitingList) == 0 {
		return true
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// re-check in case it got evicted
	if _, still := c.cache[key]; !still {
		return false
	}

	c.resolveWaiting(spanID)

	return true
}

func (c *SpanLRUCache) Close() {
	close(c.stopCleanup)
}
