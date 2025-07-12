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
	"math/rand"
	"sort"
	"strconv"
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
	isHot          bool
}

// TraceCache buffers spans per trace for a rolling expiry window
// and periodically flushes complete flows with computed flowId hashes.
type TraceCache struct {
	mutex         sync.Mutex
	traces        map[string]*traceEntry
	expiry        time.Duration
	flushInterval time.Duration
	flushCallback func(spans []ptrace.Span)
	stopCh        chan struct{}

	hotTraceIDs          []string        // traceIDs considered hot
	hotIdx               map[string]int  // index of hot traceIDs for quick lookup
	seenSpanFingerprints map[int64]int64 // fingerprints of spans seen in hot flows

	hotFingerprints map[int64]struct{} // fingerprints of spans in hot flows

	numSamples int
}

// NewTraceCache creates a new TraceCache. 'expiry' controls how long
// to retain a trace entry before evicting it; 'flushInterval' controls
// how often flows are computed and emitted via flushCallback.
func NewTraceCache(
	expiry time.Duration,
	numSamples int,
	flushCallback func(spans []ptrace.Span),
) *TraceCache {
	c := &TraceCache{
		traces:               make(map[string]*traceEntry),
		expiry:               expiry,
		flushCallback:        flushCallback,
		stopCh:               make(chan struct{}),
		hotFingerprints:      make(map[int64]struct{}),
		hotTraceIDs:          make([]string, 0),
		hotIdx:               make(map[string]int),
		seenSpanFingerprints: make(map[int64]int64),
		numSamples:           numSamples,
	}
	go c.startCleaner()
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
			tracesToExpire := make(map[string]*traceEntry)
			for traceID, entry := range c.traces {
				if now.Sub(entry.timestamp) > c.expiry {
					tracesToExpire[traceID] = entry
				}
			}
			c.hotFingerprints = c.flush(tracesToExpire)
			for traceID := range tracesToExpire {
				c.deleteTrace(traceID)
			}

			for fingerprint, timestamp := range c.seenSpanFingerprints {
				if now.UnixMilli()-timestamp > 5*int64(c.expiry/time.Millisecond) {
					delete(c.seenSpanFingerprints, fingerprint)
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
		c.seenSpanFingerprints[fingerprint] = now.UnixMilli()
		shouldSample = true
	}

	// if still not sampling, evict one hot trace to make room
	if !shouldSample {
		if len(c.hotTraceIDs) > 0 {
			// pick a random hot trace to evict
			i := rand.Intn(len(c.hotTraceIDs))
			victim := c.hotTraceIDs[i]
			// deleteTrace handles removing from c.traces and the hot structures.
			c.deleteTrace(victim)
			shouldSample = true
		} else {
			// no hot traces to evict — skip this new trace
			return
		}
	}

	if !existsInCache {
		entry = &traceEntry{
			spans:          make([]ptrace.Span, 0),
			parents:        make(map[string]string),
			children:       make(map[string][]string),
			fingerprintMap: make(map[string]int64),
			timestamp:      now,
			isHot:          true,
		}
		c.traces[traceID] = entry
	}
	c.storeSpan(traceID, span, fingerprint, entry, now)
}

// deleteTrace removes from all state
func (c *TraceCache) deleteTrace(traceID string) {
	delete(c.traces, traceID)
	if idx, ok := c.hotIdx[traceID]; ok {
		last := c.hotTraceIDs[len(c.hotTraceIDs)-1]
		c.hotTraceIDs[idx] = last
		c.hotIdx[last] = idx
		c.hotTraceIDs = c.hotTraceIDs[:len(c.hotTraceIDs)-1]
		delete(c.hotIdx, traceID)
	}
}

func (c *TraceCache) storeSpan(traceID string, span ptrace.Span, fingerprint int64, entry *traceEntry, now time.Time) {
	// update timestamp for sliding window
	entry.timestamp = now

	sid := span.SpanID().String()
	pid := span.ParentSpanID().String()

	entry.parents[sid] = pid
	entry.fingerprintMap[sid] = fingerprint
	entry.spans = append(entry.spans, span)
	entry.children[pid] = append(entry.children[pid], sid)
	_, isHot := c.hotFingerprints[fingerprint]
	entry.isHot = entry.isHot && isHot
	if entry.isHot {
		if _, exists := c.hotIdx[traceID]; !exists {
			c.hotTraceIDs = append(c.hotTraceIDs, traceID)
			c.hotIdx[traceID] = len(c.hotTraceIDs) - 1
		}
	} else {
		if idx, exists := c.hotIdx[traceID]; exists {
			last := c.hotTraceIDs[len(c.hotTraceIDs)-1]
			c.hotTraceIDs[idx] = last
			c.hotIdx[last] = idx
			c.hotTraceIDs = c.hotTraceIDs[:len(c.hotTraceIDs)-1]
			delete(c.hotIdx, traceID)
		}
	}
}

// flush computes a flowId per root-based DFS and emits via flushCallback
func (c *TraceCache) flush(tracesToExpire map[string]*traceEntry) map[int64]struct{} {
	seenFlowIDs := make(map[string]struct{})
	spansByFingerprint := make(map[int64]ptrace.Span)
	spansToFlush := make([]ptrace.Span, 0)
	frequencyByFlowId := make(map[string]int)
	spanFingerprintsByFlowId := make(map[string]map[int64]struct{})

	for _, entry := range tracesToExpire {
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
			// fallback to the span with the earliest timestamp
			sort.Slice(entry.spans, func(i, j int) bool {
				return entry.spans[i].StartTimestamp() < entry.spans[j].StartTimestamp()
			})
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
			frequencyByFlowId[flowID]++
			spanFingerprintsByFlowId[flowID] = make(map[int64]struct{})

			if _, dup := seenFlowIDs[flowID]; dup {
				continue
			}
			seenFlowIDs[flowID] = struct{}{}

			for _, sp := range entry.spans {
				fingerprint := fingerprinter.GetFingerprintAttribute(sp.Attributes())
				spanFingerprintsByFlowId[flowID][fingerprint] = struct{}{}

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
						var parentFingerprintMap pcommon.Map
						parentFingerprintVal, parentFingerprintExists := spanToModify.Attributes().Get("parent.fingerprints")
						if parentFingerprintExists {
							parentFingerprintMap = parentFingerprintVal.Map()
						} else {
							parentFingerprintMap = spanToModify.Attributes().PutEmptyMap("parent.fingerprints")
						}
						parentFingerprintMap.PutStr(strconv.FormatInt(parentFingerprint, 10), "")
					}
				}
			}
		}
	}

	for _, span := range spansByFingerprint {
		spansToFlush = append(spansToFlush, span)
	}

	c.flushCallback(spansToFlush)

	// identify hot flows, and add them to hotFingerprints
	hotFingerprints := make(map[int64]struct{})
	for flowID, count := range frequencyByFlowId {
		if count >= int(0.8*float64(len(c.traces))) {
			if fingerprints, ok := spanFingerprintsByFlowId[flowID]; ok {
				for fingerprint := range fingerprints {
					hotFingerprints[fingerprint] = struct{}{}
				}
			}
		}
	}
	return hotFingerprints
}

// Close stops background routines
func (c *TraceCache) Close() {
	close(c.stopCh)
}
