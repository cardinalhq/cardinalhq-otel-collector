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
	"math/rand"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type traceEntry struct {
	spans   []ptrace.Span
	parents map[string]string // spanID -> parentSpanID
}

type traceBucket struct {
	sample map[string]struct{}
}

type TraceCache struct {
	mutex          sync.Mutex
	buckets        map[int]*traceBucket
	traces         map[string]*traceEntry
	flushCallback  func(traceID string, spans []ptrace.Span)
	stopCh         chan struct{}
	flushInterval  time.Duration
	numBuckets     int
	perBucketLimit int
}

func NewTraceCache(flushInterval time.Duration, numBuckets, perBucketLimit int, flushCallback func(traceID string, spans []ptrace.Span)) *TraceCache {
	t := &TraceCache{
		flushInterval:  flushInterval,
		numBuckets:     numBuckets,
		perBucketLimit: perBucketLimit,
		flushCallback:  flushCallback,
		buckets:        make(map[int]*traceBucket),
		traces:         make(map[string]*traceEntry),
		stopCh:         make(chan struct{}),
	}
	for i := 0; i < numBuckets; i++ {
		t.buckets[i] = &traceBucket{sample: make(map[string]struct{})}
	}
	go t.runFlusher()
	return t
}

func (t *TraceCache) Put(span ptrace.Span, fingerprint int64) {
	traceID := span.TraceID().String()
	bucketIdx := rand.Intn(t.numBuckets)

	t.mutex.Lock()
	defer t.mutex.Unlock()

	bucket := t.buckets[bucketIdx]
	if _, exists := bucket.sample[traceID]; exists {
		t.storeSpan(traceID, span)
		return
	}

	if len(bucket.sample) < t.perBucketLimit && rand.Float64() < 0.5 {
		bucket.sample[traceID] = struct{}{}
		t.storeSpan(traceID, span)
		return
	}
	//TODO: Maintain a map of span fingerprints to check if we have seen this fingerprint before, if not, then add the corresponding traceId to the cache anyway
}

func (t *TraceCache) storeSpan(traceID string, span ptrace.Span) {
	entry, exists := t.traces[traceID]
	if !exists {
		entry = &traceEntry{
			spans:   make([]ptrace.Span, 0),
			parents: make(map[string]string),
		}
		t.traces[traceID] = entry
	}
	spanID := span.SpanID().String()
	parentID := span.ParentSpanID().String()
	entry.parents[spanID] = parentID
	entry.spans = append(entry.spans, span)
}

func (t *TraceCache) runFlusher() {
	ticker := time.NewTicker(t.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.flush()
		case <-t.stopCh:
			return
		}
	}
}

func (t *TraceCache) flush() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for traceID, entry := range t.traces {
		t.flushCallback(traceID, entry.spans)
		delete(t.traces, traceID)
	}
	for _, bucket := range t.buckets {
		bucket.sample = make(map[string]struct{})
	}
}

func (t *TraceCache) Close() {
	close(t.stopCh)
}
