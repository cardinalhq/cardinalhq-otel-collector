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
	"encoding/hex"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// makeSpan constructs a ptrace.Span with given traceID, spanID, parentID hex strings.
func makeSpan(traceHex, spanHex, parentHex string) (ptrace.Span, error) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	sp := ss.Spans().AppendEmpty()

	// set traceID (16 bytes)
	traceBytes, err := hex.DecodeString(traceHex)
	if err != nil {
		return ptrace.Span{}, err
	}
	var traceArr pcommon.TraceID
	copy(traceArr[:], traceBytes)
	sp.SetTraceID(traceArr)

	// set spanID (8 bytes)
	spanBytes, err := hex.DecodeString(spanHex)
	if err != nil {
		return ptrace.Span{}, err
	}
	var spanArr pcommon.SpanID
	copy(spanArr[:], spanBytes)
	sp.SetSpanID(spanArr)

	// set parentSpanID if provided
	if parentHex != "" {
		parentBytes, err := hex.DecodeString(parentHex)
		if err != nil {
			return ptrace.Span{}, err
		}
		var parentArr pcommon.SpanID
		copy(parentArr[:], parentBytes)
		sp.SetParentSpanID(parentArr)
	}

	return sp, nil
}

func TestTraceCache_TwoCallGraphs(t *testing.T) {
	// setup cache
	expiry := time.Minute
	flushInterval := time.Minute
	numSamples := 10
	var calls [][]ptrace.Span
	cache := NewTraceCache(expiry, numSamples, flushInterval, func(spans []ptrace.Span) {
		calls = append(calls, spans)
	})

	// define two traces
	trace1 := "00000000000000000000000000000001"
	trace2 := "00000000000000000000000000000002"

	// span IDs
	// A->B->C uses spanIDs 0100000000000000,0200000000000000,0300000000000000
	// second call graph reuses same A spanID for continuity
	spanA := "0100000000000000"
	spanB := "0200000000000000"
	spanC := "0300000000000000"
	spanD := "0400000000000000"
	spanE := "0500000000000000"

	// create spans and assign fingerprint attribute same as last byte
	s1, err := makeSpan(trace1, spanA, "")
	if err != nil {
		t.Fatal(err)
	}
	s1.Attributes().PutInt(translate.CardinalFieldFingerprint, 1)
	s2, err := makeSpan(trace1, spanB, spanA)
	if err != nil {
		t.Fatal(err)
	}
	s2.Attributes().PutInt(translate.CardinalFieldFingerprint, 2)
	s3, err := makeSpan(trace1, spanC, spanB)
	if err != nil {
		t.Fatal(err)
	}
	s3.Attributes().PutInt(translate.CardinalFieldFingerprint, 3)

	s4, err := makeSpan(trace2, spanA, "")
	if err != nil {
		t.Fatal(err)
	}
	s4.Attributes().PutInt(translate.CardinalFieldFingerprint, 1)
	s5, err := makeSpan(trace2, spanD, spanA)
	if err != nil {
		t.Fatal(err)
	}
	s5.Attributes().PutInt(translate.CardinalFieldFingerprint, 4)
	s6, err := makeSpan(trace2, spanE, spanD)
	if err != nil {
		t.Fatal(err)
	}
	s6.Attributes().PutInt(translate.CardinalFieldFingerprint, 5)

	// ingest spans
	cache.Put(s1, 1)
	cache.Put(s2, 2)
	cache.Put(s3, 3)
	cache.Put(s4, 1)
	cache.Put(s5, 4)
	cache.Put(s6, 5)

	// manually flush
	cache.flush()

	if len(calls) != 1 {
		t.Fatalf("expected single flush call, got %d", len(calls))
	}
	sl := calls[0]

	// count flowIds per fingerprint
	counts := make(map[int64]int)
	for _, sp := range sl {
		// extract fingerprint
		fp := fingerprinter.GetFingerprintAttribute(sp.Attributes())
		// extract flowId map and count its entries
		val, _ := sp.Attributes().Get("flowId")
		flowMap := val.Map()
		count := 0
		flowMap.Range(func(k string, v pcommon.Value) bool {
			count++
			return true
		})
		counts[fp] = count
	}

	expected := map[int64]int{
		1: 2,
		2: 1,
		3: 1,
		4: 1,
		5: 1,
	}

	for fp, exp := range expected {
		if counts[fp] != exp {
			t.Errorf("fingerprint %d: expected %d flowIds, got %d", fp, exp, counts[fp])
		}
	}
}
