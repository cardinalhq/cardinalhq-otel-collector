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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// traceEntry holds all spans and relationships for a given trace
type traceEntry struct {
	spans          []ptrace.Span
	parents        map[string]string   // spanID -> parentSpanID
	children       map[string][]string // parentSpanID -> []spanID
	fingerprintMap map[string]int64    // spanID -> fingerprint
	timestamp      time.Time           // first seen or last update
}

// TraceCache buffers spans per trace for a rolling expiry window
// and periodically flushes complete flows with computed flowId hashes.
type TraceCache struct {
	mutex                sync.Mutex
	traces               map[string]*traceEntry
	expiry               time.Duration
	flushInterval        time.Duration
	flushCallback        func(spans []ptrace.Span)
	stopCh               chan struct{}
	seenSpanFingerprints map[int64]struct{}
	numSamples           int
}

// NewTraceCache creates a new TraceCache. 'expiry' controls how long
// to retain a trace entry before evicting it; 'flushInterval' controls
// how often flows are computed and emitted via flushCallback.
func NewTraceCache(
	expiry time.Duration,
	numSamples int,
	flushInterval time.Duration,
	flushCallback func(spans []ptrace.Span),
) *TraceCache {
	c := &TraceCache{
		traces:               make(map[string]*traceEntry),
		expiry:               expiry,
		flushInterval:        flushInterval,
		flushCallback:        flushCallback,
		stopCh:               make(chan struct{}),
		seenSpanFingerprints: make(map[int64]struct{}),
		numSamples:           numSamples,
	}
	go c.startCleaner()
	go c.runFlusher()
	return c
}

// startCleaner evicts trace entries older than 'expiry'
func (c *TraceCache) startCleaner() {
	ticker := time.NewTicker(c.expiry)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			c.mutex.Lock()
			for tid, entry := range c.traces {
				if now.Sub(entry.timestamp) > c.expiry {
					delete(c.traces, tid)
				}
			}
			c.mutex.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

// Put ingests a span into the cache, recording its parent and fingerprint
func (c *TraceCache) Put(span ptrace.Span, fingerprint int64) {
	traceID := span.TraceID().String()
	now := time.Now()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	entry, existsInCache := c.traces[traceID]
	shouldSample := existsInCache

	if !shouldSample && len(c.traces) < c.numSamples {
		shouldSample = true
	}

	if _, exists := c.seenSpanFingerprints[fingerprint]; !shouldSample && !exists {
		c.seenSpanFingerprints[fingerprint] = struct{}{}
		shouldSample = true
	}

	if !shouldSample {
		return
	}

	if !existsInCache {
		entry = &traceEntry{
			spans:          make([]ptrace.Span, 0),
			parents:        make(map[string]string),
			children:       make(map[string][]string),
			fingerprintMap: make(map[string]int64),
			timestamp:      now,
		}
		c.traces[traceID] = entry
	}
	// update timestamp for sliding window
	entry.timestamp = now

	sid := span.SpanID().String()
	pid := span.ParentSpanID().String()

	entry.parents[sid] = pid
	entry.fingerprintMap[sid] = fingerprint
	entry.spans = append(entry.spans, span)
	entry.children[pid] = append(entry.children[pid], sid)
}

// runFlusher triggers flow computation every flushInterval
func (c *TraceCache) runFlusher() {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.flush()
		case <-c.stopCh:
			return
		}
	}
}

// flush computes a flowId per root-based DFS and emits via flushCallback
func (c *TraceCache) flush() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	seenFlowIDs := make(map[string]struct{})
	spansByFingerprint := make(map[int64]ptrace.Span)
	spansToFlush := make([]ptrace.Span, 0)

	for _, entry := range c.traces {
		// find root spanIDs: those whose parent is empty or missing
		roots := make([]string, 0)
		for sid, pid := range entry.parents {
			if pid == "" {
				roots = append(roots, sid)
			} else if _, exists := entry.parents[pid]; !exists {
				roots = append(roots, sid)
			}
		}
		// if no explicit roots, pick the first span
		if len(roots) == 0 && len(entry.spans) > 0 {
			roots = append(roots, entry.spans[0].SpanID().String())
		}

		for _, root := range roots {
			var order []int64
			var dfs func(string)
			dfs = func(cur string) {
				if fp, ok := entry.fingerprintMap[cur]; ok {
					order = append(order, fp)
				}
				kids := entry.children[cur]
				sort.Strings(kids)
				for _, child := range kids {
					dfs(child)
				}
			}
			dfs(root)

			h := sha256.New()
			for _, fp := range order {
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(fp))
				h.Write(buf[:])
			}
			flowID := hex.EncodeToString(h.Sum(nil))

			if _, dup := seenFlowIDs[flowID]; dup {
				continue
			}
			seenFlowIDs[flowID] = struct{}{}

			for _, sp := range entry.spans {
				fingerprint := fingerprinter.GetFingerprintAttribute(sp.Attributes())
				// check if spansByFingerprint already has this fingerprint
				var spanToModify ptrace.Span
				if s, exists := spansByFingerprint[fingerprint]; exists {
					spanToModify = s
				} else {
					spanToModify = sp
					spansByFingerprint[fingerprint] = sp
				}
				var flowIdMap pcommon.Map
				flowIdVal, flowIdValExists := spanToModify.Attributes().Get("flowId")
				if flowIdValExists {
					flowIdMap = flowIdVal.Map()
				} else {
					flowIdMap = spanToModify.Attributes().PutEmptyMap("flowId")
				}
				flowIdMap.PutStr(flowID, "")

				sid := sp.SpanID().String()
				if parentID, exists := entry.parents[sid]; exists && parentID != "" {
					if parentFingerprint, parentExists := entry.fingerprintMap[parentID]; parentExists {
						spanToModify.Attributes().PutInt("parent.fingerprint", parentFingerprint)
					}
				}

			}

		}
	}

	for _, span := range spansByFingerprint {
		spansToFlush = append(spansToFlush, span)
	}

	c.flushCallback(spansToFlush)
}

// Close stops background routines
func (c *TraceCache) Close() {
	close(c.stopCh)
}
