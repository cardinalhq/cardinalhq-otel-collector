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
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// waitItem pairs a SpanEntry with its enqueue timestamp while awaiting its parent.
type waitItem struct {
	entry    *SpanEntry
	enqueued time.Time
}

// SpanEntry represents a single span exemplar and its relationship info.
type SpanEntry struct {
	SpanID            string
	Key               int64
	Fingerprint       int64
	ParentFingerprint int64
	ParentSpanID      string
	Attributes        []string
	Exemplar          ptrace.Traces
	Timestamp         time.Time
	LastPublishTime   time.Time
}

// toAttributes returns a map of this span's attributes, plus the parent fingerprint.
func (e *SpanEntry) toAttributes() map[string]string {
	attrs := make(map[string]string)
	for i := 0; i+1 < len(e.Attributes); i += 2 {
		attrs[e.Attributes[i]] = e.Attributes[i+1]
	}
	attrs["parent.fingerprint"] = strconv.FormatInt(e.ParentFingerprint, 10)
	return attrs
}

// SpanCache holds span exemplars, resolves parent-child links, and evicts old entries.
type SpanCache struct {
	entries             map[int64]*SpanEntry  // active entries by exemplar key
	spanIdToFingerprint map[string]int64      // resolved spanID → fingerprint
	waiting             map[string][]waitItem // parentSpanID → children queued
	expiry              time.Duration         // TTL for entries before eviction
	reportInterval      time.Duration         // how often to publish & clean
	mutex               sync.RWMutex          // protects all maps & slices
	stopCleanup         chan struct{}         // signals the cleanup goroutine to exit
	publishCallback     func([]*SpanEntry)    // called with entries to publish
}

// NewSpanCache creates a cache that publishes new entries and evicts old ones.
func NewSpanCache(
	expiry, reportInterval time.Duration,
	publishCallback func([]*SpanEntry),
) *SpanCache {
	c := &SpanCache{
		entries:             make(map[int64]*SpanEntry),
		spanIdToFingerprint: make(map[string]int64),
		waiting:             make(map[string][]waitItem),
		expiry:              expiry,
		reportInterval:      reportInterval,
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

// cleanupExpired publishes new entries, evicts expired ones, and prunes stale waiting items.
func (c *SpanCache) cleanupExpired() {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 2) Evict expired entries
	expired := make([]*SpanEntry, 0)
	for key, entry := range c.entries {
		if now.Sub(entry.Timestamp) > c.expiry {
			expired = append(expired, entry)
			delete(c.entries, key)
			delete(c.spanIdToFingerprint, entry.SpanID)
			// drop any children waiting on this span
			delete(c.waiting, entry.SpanID)
		}
	}
	if len(expired) > 0 {
		c.publishCallback(expired)
	}

	// 3) Prune waiting items older than expiry
	for pid, items := range c.waiting {
		live := items[:0]
		for _, wi := range items {
			if now.Sub(wi.enqueued) < c.expiry {
				live = append(live, wi)
			}
		}
		if len(live) > 0 {
			c.waiting[pid] = live
		} else {
			delete(c.waiting, pid)
		}
	}
}

// Put adds or updates a span entry, links any queued children, and queues if its parent isn't seen.
func (c *SpanCache) Put(
	key int64,
	spanID, parentSpanID string,
	fingerprint int64,
	exemplar ptrace.Traces,
	attributes []string,
) {
	now := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entry := &SpanEntry{
		SpanID:          spanID,
		ParentSpanID:    parentSpanID,
		Key:             key,
		Fingerprint:     fingerprint,
		Attributes:      attributes,
		Exemplar:        exemplar,
		Timestamp:       now,
		LastPublishTime: now,
	}
	c.entries[key] = entry

	// 2) Record our fingerprint & resolve any queued children
	c.spanIdToFingerprint[spanID] = fingerprint
	c.resolveWaiting(spanID)

	// 3) Link to parent or queue this entry
	if parentSpanID != "" {
		if pf, found := c.spanIdToFingerprint[parentSpanID]; found {
			entry.ParentFingerprint = pf
		} else {
			c.waiting[parentSpanID] = append(c.waiting[parentSpanID], waitItem{
				entry:    entry,
				enqueued: now,
			})
		}
	}
}

// resolveWaiting stamps ParentFingerprint on any children waiting for spanID.
func (c *SpanCache) resolveWaiting(spanID string) {
	items, ok := c.waiting[spanID]
	if !ok {
		return
	}
	pf := c.spanIdToFingerprint[spanID]
	for _, wi := range items {
		wi.entry.ParentFingerprint = pf
	}
	delete(c.waiting, spanID)
}

// Contains checks if an entry exists and resolves any waiting children for spanID.
func (c *SpanCache) Contains(
	spanID string,
	fingerprint int64,
	key int64,
) bool {
	c.mutex.RLock()
	_, inCache := c.entries[key]
	items, hasWaiting := c.waiting[spanID]
	c.mutex.RUnlock()

	if !inCache {
		return false
	}

	if hasWaiting {
		c.mutex.Lock()
		for _, wi := range items {
			wi.entry.ParentFingerprint = fingerprint
		}
		delete(c.waiting, spanID)
		c.mutex.Unlock()
	}
	return true
}

// Close stops the background cleanup goroutine.
func (c *SpanCache) Close() {
	close(c.stopCleanup)
}
