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
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type SpanEntry struct {
	key             int64
	fingerprint     int64
	attributes      []string
	exemplar        ptrace.Traces
	timestamp       time.Time
	lastPublishTime time.Time
}

type SpanLRUCache struct {
	capacity       int
	expiry         time.Duration
	reportInterval time.Duration

	cache map[int64]*list.Element
	list  *list.List
	mutex sync.RWMutex

	pending         []*SpanEntry
	stopCleanup     chan struct{}
	publishCallback func([]*SpanEntry)
}

func NewSpanLRUCache(capacity int, expiry, reportInterval time.Duration, publishCallback func([]*SpanEntry)) *SpanLRUCache {
	c := &SpanLRUCache{
		capacity:        capacity,
		expiry:          expiry,
		reportInterval:  reportInterval,
		cache:           make(map[int64]*list.Element),
		list:            list.New(),
		stopCleanup:     make(chan struct{}),
		publishCallback: publishCallback,
		pending:         make([]*SpanEntry, 0),
	}
	go c.startCleanup()
	return c
}

func (e *SpanEntry) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i < len(e.attributes); i += 2 {
		attrs[e.attributes[i]] = e.attributes[i+1]
	}
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

func (c *SpanLRUCache) cleanupExpired() {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for e := c.list.Back(); e != nil; {
		prev := e.Prev()
		entry := e.Value.(*SpanEntry)

		if now.Sub(entry.lastPublishTime) >= c.expiry {
			c.pending = append(c.pending, entry)
			entry.lastPublishTime = now
		}

		if now.Sub(entry.timestamp) > c.expiry {
			c.list.Remove(e)
			delete(c.cache, entry.key)
		}
		e = prev
	}

	if len(c.pending) > 0 {
		stampParentFingerprints(c.pending)
		c.publishCallback(c.pending)
		c.pending = c.pending[:0]
	}
}

func stampParentFingerprints(entries []*SpanEntry) {
	spanIDToFingerprint := make(map[string]int64)
	for _, entry := range entries {
		if rls := entry.exemplar.ResourceSpans(); rls.Len() > 0 {
			sl := rls.At(0).ScopeSpans()
			if sl.Len() > 0 {
				spans := sl.At(0).Spans()
				for i := 0; i < spans.Len(); i++ {
					span := spans.At(i)
					spanIDToFingerprint[span.SpanID().String()] = entry.fingerprint
				}
			}
		}
	}

	for _, entry := range entries {
		if rls := entry.exemplar.ResourceSpans(); rls.Len() > 0 {
			sl := rls.At(0).ScopeSpans()
			if sl.Len() > 0 {
				spans := sl.At(0).Spans()
				for i := 0; i < spans.Len(); i++ {
					span := spans.At(i)
					if !span.ParentSpanID().IsEmpty() {
						parentID := span.ParentSpanID().String()
						if parentFp, exists := spanIDToFingerprint[parentID]; exists {
							span.Attributes().PutInt("parent.fingerprint", parentFp)
						}
					}
				}
			}
		}
	}
}

func (c *SpanLRUCache) Put(key int64, fingerprint int64, exemplar ptrace.Traces, attributes []string) {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.list.MoveToFront(elem)
		existing := elem.Value.(*SpanEntry)
		existing.exemplar = exemplar
		existing.timestamp = now
		return
	}

	if c.list.Len() >= c.capacity {
		back := c.list.Back()
		if back != nil {
			e := back.Value.(*SpanEntry)
			if time.Since(e.lastPublishTime) >= c.expiry {
				c.pending = append(c.pending, e)
				e.lastPublishTime = now
			}
			c.list.Remove(back)
			delete(c.cache, e.key)
		}
	}

	entry := &SpanEntry{
		key:             key,
		fingerprint:     fingerprint,
		exemplar:        exemplar,
		attributes:      attributes,
		timestamp:       now,
		lastPublishTime: now,
	}
	elem := c.list.PushFront(entry)
	c.cache[key] = elem
	c.pending = append(c.pending, entry)
}

func (c *SpanLRUCache) ContainsByKey(key int64) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, ok := c.cache[key]
	return ok
}

func (c *SpanLRUCache) Close() {
	close(c.stopCleanup)
}
