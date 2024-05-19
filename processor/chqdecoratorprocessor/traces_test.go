// Copyright 2024 CardinalHQ, Inc
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

package chqdecoratorprocessor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/honeycombio/dynsampler-go"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/spantagger"
)

func TestNewTrace(t *testing.T) {
	sp := &spansProcessor{
		logger: zap.NewNop(),
		sentFingerprints: fingerprintTracker{
			fingerprints: make(map[uint64]struct{}),
		},
		telemetry: &processorTelemetry{},
	}

	fingerprint := uint64(1234567890)

	// First call with a new fingerprint should return true
	if !sp.newTrace(fingerprint) {
		t.Error("expected newTrace to return true")
	}

	// Second call with the same fingerprint should return false
	if sp.newTrace(fingerprint) {
		t.Error("expected newTrace to return false")
	}
}

func TestDeleteTrace(t *testing.T) {
	sp := &spansProcessor{
		logger: zap.NewNop(),
		sentFingerprints: fingerprintTracker{
			fingerprints: make(map[uint64]struct{}),
		},
	}

	fingerprint := uint64(1234567890)

	// Add the fingerprint to the sentFingerprints map
	sp.sentFingerprints.fingerprints[fingerprint] = struct{}{}

	// Call the deleteTrace function
	sp.deleteTrace(fingerprint)

	// Check if the fingerprint is removed from the sentFingerprints map
	_, ok := sp.sentFingerprints.fingerprints[fingerprint]
	if ok {
		t.Error("expected fingerprint to be deleted")
	}
}

func TestSendGraph(t *testing.T) {
	// Create a mock server to receive the HTTP request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request method and URL
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.URL.String() != "/graph" {
			t.Errorf("expected URL /graph, got %s", r.URL.String())
		}

		// Verify the request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		expectedBody := `{"fingerprint":1234567890}`
		if string(body) != expectedBody {
			t.Errorf("expected request body %s, got %s", expectedBody, string(body))
		}

		// Send a successful response
		w.WriteHeader(http.StatusOK)
	}))

	// Create a spansProcessor instance with the mock server URL
	sp := &spansProcessor{
		logger:    zap.NewNop(),
		telemetry: &processorTelemetry{},
		traceConfig: &TraceConfig{
			GraphURL: server.URL + "/graph",
		},
	}

	// Create a mock graph
	graph := &spantagger.Graph{
		Fingerprint: 1234567890,
	}

	// Call the sendGraph function
	err := sp.sendGraph(context.Background(), graph)
	if err != nil {
		t.Errorf("sendGraph failed: %v", err)
	}

	// Close the mock server
	server.Close()
}

func TestShouldFilter(t *testing.T) {
	type args struct {
		td          ptrace.Traces
		fingerprint uint64
		hasErr      bool
	}

	type test struct {
		name           string
		args           args
		filtered       bool
		classification filteredReason
	}

	tests := []test{
		{
			"InvalidFingerprint",
			args{
				func() ptrace.Traces {
					td := ptrace.NewTraces()
					return td
				}(),
				0,
				false,
			},
			true,
			"invalid_fingerprint",
		},
		{
			"HasError",
			args{
				func() ptrace.Traces {
					td := ptrace.NewTraces()
					return td
				}(),
				1234567890,
				true,
			},
			false,
			"trace_has_error",
		},
		{
			"uninteresting",
			args{
				func() ptrace.Traces {
					td := ptrace.NewTraces()
					rs := td.ResourceSpans().AppendEmpty()
					ss := rs.ScopeSpans().AppendEmpty()
					span := ss.Spans().AppendEmpty()
					span.SetName("uninteresting")
					span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6})
					span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
					return td
				}(),
				1234567890,
				false,
			},
			false,
			"uninteresting",
		},
		{
			"slow",
			args{
				func() ptrace.Traces {
					td := ptrace.NewTraces()
					rs := td.ResourceSpans().AppendEmpty()
					ss := rs.ScopeSpans().AppendEmpty()
					span := ss.Spans().AppendEmpty()
					span.SetName("interesting")
					span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6})
					span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
					span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
					span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(100000, 0)))
					return td
				}(),
				1234567890,
				false,
			},
			false,
			"slow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &spansProcessor{
				logger:              zap.NewNop(),
				telemetry:           &processorTelemetry{},
				estimators:          make(map[uint64]*OnlineWindowStat),
				estimatorWindowSize: 10,
			}
			sketch := sp.findSketch(tt.args.fingerprint)
			for i := 0; i < 10; i++ {
				sketch.Update(10000)
			}

			filtered, classification := sp.shouldFilter(tt.args.td, tt.args.fingerprint, tt.args.hasErr)
			assert.Equal(t, tt.filtered, filtered)
			assert.Equal(t, tt.classification, classification)
		})
	}
}

func TestRateLimitSlow(t *testing.T) {
	sp := &spansProcessor{
		logger:      zap.NewNop(),
		slowSampler: constantSampler(1),
	}

	fingerprint := uint64(1234567890)

	isSlow := sp.rateLimitSlow(fingerprint)
	assert.False(t, isSlow)

	sp.slowSampler = constantSampler(0)
	isSlow = sp.rateLimitSlow(fingerprint)
	assert.True(t, isSlow)
}

type constantSamplerMock struct {
	rate int
}

var _ dynsampler.Sampler = (*constantSamplerMock)(nil)

func constantSampler(rate int) dynsampler.Sampler {
	return &constantSamplerMock{
		rate: rate,
	}
}

func (s *constantSamplerMock) GetSampleRate(key string) int {
	return s.rate
}

func (s *constantSamplerMock) GetSampleRateMulti(_ string, _ int) int {
	return s.rate
}

func (s *constantSamplerMock) Start() error {
	return nil
}

func (s *constantSamplerMock) Stop() error {
	return nil
}

func (s *constantSamplerMock) GetMetrics(_ string) map[string]int64 {
	return nil
}

func (s *constantSamplerMock) LoadState(_ []byte) error {
	return nil
}

func (s *constantSamplerMock) SaveState() ([]byte, error) {
	return nil, nil
}

func TestFindRootDuration(t *testing.T) {
	// Create test cases
	tests := []struct {
		name     string
		traces   ptrace.Traces
		expected int64
		found    bool
	}{
		{
			name: "EmptyTraces",
			traces: func() ptrace.Traces {
				td := ptrace.NewTraces()
				return td
			}(),
			expected: 0,
			found:    false,
		},
		{
			name: "RootSpanFound",
			traces: func() ptrace.Traces {
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				ss := rs.ScopeSpans().AppendEmpty()
				span := ss.Spans().AppendEmpty()
				span.SetParentSpanID(pcommon.SpanID([8]byte{}))
				span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(1 * time.Second)))
				return td
			}(),
			expected: 1000,
			found:    true,
		},
		{
			name: "NoRootSpan",
			traces: func() ptrace.Traces {
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				ss := rs.ScopeSpans().AppendEmpty()
				span := ss.Spans().AppendEmpty()
				span.SetParentSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
				span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(1 * time.Second)))
				return td
			}(),
			expected: 0,
			found:    false,
		},
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration, found := findRootDuration(tt.traces)
			assert.Equal(t, tt.expected, duration)
			assert.Equal(t, tt.found, found)
		})
	}
}

func TestMaybeRateLimit(t *testing.T) {
	type args struct {
		fingerprint    uint64
		filtered       bool
		filteredReason filteredReason
	}

	tests := []struct {
		name     string
		args     args
		expected bool
	}{
		{
			"Filtered",
			args{
				1234567890,
				true,
				"uninterestingPassthrough",
			},
			true,
		},
		{
			"HasError",
			args{
				1234567890,
				false,
				filteredReasonTraceHasError,
			},
			true,
		},
		{
			"Slow",
			args{
				1234567890,
				false,
				filteredReasonSlow,
			},
			true,
		},
		{
			"Uninteresting",
			args{
				1234567890,
				false,
				filteredReasonUninteresting,
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &spansProcessor{
				logger:               zap.NewNop(),
				telemetry:            &processorTelemetry{},
				slowSampler:          constantSampler(0),
				hasErrorSampler:      constantSampler(0),
				uninterestingSampler: constantSampler(0),
			}

			rateLimited := sp.maybeRateLimit(tt.args.fingerprint, tt.args.filtered, tt.args.filteredReason)
			assert.Equal(t, tt.expected, rateLimited)
		})
	}
}

func TestRateHelper(t *testing.T) {
	// Test case: rate = 0, expected: true
	rate := 0
	expected := true
	result := rateHelper(rate)
	if result != expected {
		t.Errorf("rateHelper(%d) = %t, expected %t", rate, result, expected)
	}

	// Test case: rate = 1, expected: false
	rate = 1
	expected = false
	result = rateHelper(rate)
	if result != expected {
		t.Errorf("rateHelper(%d) = %t, expected %t", rate, result, expected)
	}

	trues := 0
	falses := 0
	for i := 0; i < 100_000; i++ {
		result = rateHelper(100)
		if result {
			trues++
		} else {
			falses++
		}
	}
	if trues < 80_000 {
		t.Errorf("rateHelper(100) should return false more often than true: trues=%d, falses=%d", trues, falses)
	}
}
