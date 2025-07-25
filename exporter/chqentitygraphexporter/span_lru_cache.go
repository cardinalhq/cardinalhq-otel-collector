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
	"log/slog"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type SpanEntry struct {
	SpanID            string
	Key               int64
	Fingerprint       int64
	ParentFingerprint int64
	Attributes        []string
	Exemplar          ptrace.Traces
	Timestamp         time.Time
	LastPublishTime   time.Time
}

func (e *SpanEntry) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i < len(e.Attributes); i += 2 {
		attrs[e.Attributes[i]] = e.Attributes[i+1]
	}
	attrs["parent.fingerprint"] = strconv.FormatInt(e.ParentFingerprint, 10)
	return attrs
}

type SpanCache struct {
	// entries holds active span entries by exemplar key
	entries map[int64]*SpanEntry

	// TTL for entries before eviction
	expiry time.Duration

	// how often to run cleanup and publish
	reportInterval time.Duration

	// synchronization
	mutex sync.RWMutex

	// maps and queues for parent-child resolution
	spanIdToFingerprint map[string]int64
	waiting             map[string][]*SpanEntry

	// cleanup control
	stopCleanup     chan struct{}
	publishCallback func([]*SpanEntry)
}

// NewSpanCache creates a cache that evicts entries older than expiry and
// publishes pending spans every reportInterval.
func NewSpanCache(expiry, reportInterval time.Duration, publishCallback func([]*SpanEntry)) *SpanCache {
	c := &SpanCache{
		entries:             make(map[int64]*SpanEntry),
		expiry:              expiry,
		reportInterval:      reportInterval,
		spanIdToFingerprint: make(map[string]int64),
		waiting:             make(map[string][]*SpanEntry),
		stopCleanup:         make(chan struct{}),
		publishCallback:     publishCallback,
	}
	go c.startCleanup()
	return c
}

func (c *SpanCache) startCleanup() {
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

// cleanupExpired evicts entries older than expiry and then publishes all pending entries.
func (c *SpanCache) cleanupExpired() {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// evict old entries
	pending := make([]*SpanEntry, 0)
	for key, entry := range c.entries {
		if now.Sub(entry.Timestamp) > c.expiry {
			pending = append(pending, entry)
			// remove resolution maps
			delete(c.spanIdToFingerprint, entry.SpanID)
			delete(c.waiting, entry.SpanID)
			delete(c.entries, key)
		}
	}

	if len(pending) > 0 {
		c.publishCallback(pending)
	}
}

// Put adds or updates a span entry in the cache, stamping parent linkage when available.
func (c *SpanCache) Put(spanID, parentSpanID string, fingerprint int64, exemplar ptrace.Traces, attributes []string) {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entry := &SpanEntry{
		SpanID:          spanID,
		Key:             fingerprint,
		Fingerprint:     fingerprint,
		Attributes:      attributes,
		Exemplar:        exemplar,
		Timestamp:       now,
		LastPublishTime: now,
	}
	// store or overwrite
	c.entries[fingerprint] = entry

	// record fingerprint for parent resolution
	c.spanIdToFingerprint[spanID] = fingerprint
	c.resolveWaiting(spanID)

	// resolve this entry's parent if known
	if pf, found := c.spanIdToFingerprint[parentSpanID]; found {
		entry.ParentFingerprint = pf
	} else if parentSpanID != "" {
		// queue until parent appears
		c.waiting[parentSpanID] = append(c.waiting[parentSpanID], entry)
	}
}

// resolveWaiting stamps any queued children waiting on spanID
func (c *SpanCache) resolveWaiting(spanID string) {
	children, ok := c.waiting[spanID]
	if !ok {
		return
	}
	pf := c.spanIdToFingerprint[spanID]
	for _, child := range children {
		child.ParentFingerprint = pf
	}
	delete(c.waiting, spanID)
}

// Contains returns true if an entry exists for key, and attempts to resolve any waiting children.
func (c *SpanCache) Contains(spanID string, fingerprint int64) bool {
	c.mutex.RLock()
	_, inCache := c.entries[fingerprint]
	waitingChildren, hasWaiting := c.waiting[spanID]
	c.mutex.RUnlock()

	if !inCache {
		return false
	}

	// if children were waiting, stamp them now
	if hasWaiting {
		c.mutex.Lock()
		for _, child := range waitingChildren {
			if child.Fingerprint == 4701236566887954423 {
				slog.Info("Resolved parent for hero span", "parentFingerprint", fingerprint)
			}
			child.ParentFingerprint = fingerprint
		}
		delete(c.waiting, spanID)
		c.mutex.Unlock()
	}

	return true
}

// Close stops the background cleanup goroutine
func (c *SpanCache) Close() {
	close(c.stopCleanup)
}
