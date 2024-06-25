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

package chqenforcerprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/spantagger"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (e *chqEnforcer) ConsumeTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return td, nil
}

func (e *chqEnforcer) findSketch(fingerprint uint64) *SlidingEstimatorStat {
	e.estimatorLock.Lock()
	defer e.estimatorLock.Unlock()
	sketch, ok := e.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(e.estimatorWindowSize, e.estimatorInterval)
		e.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}

func (e *chqEnforcer) slowPercentile(fingerprint uint64, duration float64) bool {
	sketch := e.findSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

type filteredReason string

const (
	filteredReasonTraceHasError filteredReason = "trace_has_error"
	filteredReasonSlow          filteredReason = "slow"
	filteredReasonUninteresting filteredReason = "uninteresting"
	filteredReasonInvalid       filteredReason = "invalid_fingerprint"
)

func (e *chqEnforcer) shouldFilter(td ptrace.Traces, fingerprint uint64, hasError bool) (bool, filteredReason) {
	if fingerprint == 0 {
		return true, filteredReasonInvalid
	}
	slow := e.isSlow(td, fingerprint) // always call this to update our sketch
	if hasError {
		return false, filteredReasonTraceHasError
	}
	if slow {
		return false, filteredReasonSlow
	}
	return false, filteredReasonUninteresting
}

func (sp *chqEnforcer) rateLimitSlow(fingerprint uint64) bool {
	rate := sp.slowSampler.GetSampleRate(fmt.Sprintf("%d", fingerprint))
	return rateHelper(rate)
}

func (e *chqEnforcer) rateLimitHasError(fingerprint uint64) bool {
	rate := e.hasErrorSampler.GetSampleRate(fmt.Sprintf("%d", fingerprint))
	return rateHelper(rate)
}

func (e *chqEnforcer) rateLimitUninteresting(fingerprint uint64) bool {
	rate := e.uninterestingSampler.GetSampleRate(fmt.Sprintf("%d", fingerprint))
	return rateHelper(rate)
}

func (e *chqEnforcer) maybeRateLimit(fingerprint uint64, filtered bool, filteredReason filteredReason) bool {
	if filtered {
		return filtered
	}

	switch filteredReason {
	case filteredReasonSlow:
		if e.rateLimitSlow(fingerprint) {
			return true
		}
	case filteredReasonTraceHasError:
		if e.rateLimitHasError(fingerprint) {
			return true
		}
	case filteredReasonUninteresting:
		if e.rateLimitUninteresting(fingerprint) {
			return true
		}
	}

	return filtered
}

// isSlow returns true if the trace is slow compared to the 75% percentile
// of traces with the same fingerprint.
func (e *chqEnforcer) isSlow(td ptrace.Traces, fingerprint uint64) bool {
	rootDuration, found := findRootDuration(td)
	if !found {
		return false
	}
	return e.slowPercentile(fingerprint, float64(rootDuration))
}

func rateHelper(rate int) bool {
	switch rate {
	case 0:
		return true
	case 1:
		return false
	default:
		return rand.Float64() >= 1/float64(rate)
	}
}

func findRootDuration(td ptrace.Traces) (int64, bool) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.ParentSpanID().IsEmpty() {
					return span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Abs().Milliseconds(), true
				}
			}
		}
	}
	return 0, false
}

func (e *chqEnforcer) sendGraph(ctx context.Context, graph *spantagger.Graph) error {
	u := e.config.TraceConfig.GraphURL
	if u == "" {
		return nil
	}
	b, err := json.Marshal(graph)
	if err != nil {
		return fmt.Errorf("failed to marshal graph: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send graph: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send graph: http status %d", resp.StatusCode)
	}

	return nil
}

func (e *chqEnforcer) postFingerprint(ctx context.Context, td ptrace.Traces, fingerprint uint64) error {
	if fingerprint == 0 {
		return nil
	}
	if !e.newTrace(fingerprint) {
		return nil
	}
	graph, _, err := spantagger.BuildTree(td, int64(fingerprint))
	if err != nil {
		e.deleteTrace(fingerprint)
		return fmt.Errorf("failed to build graph: %w", err)
	}
	if err := e.sendGraph(ctx, graph); err != nil {
		e.deleteTrace(fingerprint)
		return fmt.Errorf("failed to send graph: %w", err)
	}

	return nil
}

// if err := sp.postFingerprint(ctx, td, fingerprint); err != nil {
//	sp.logger.Warn("failed to post fingerprint", zap.Error(err))
//}

// // First, check to see if this trace is interesting.  If it is not,
// // we will have filtered set to true.  In that case, we only want to
// // rate limit the unfiltered traces.
// filtered, filteredReason := sp.shouldFilter(td, fingerprint, hasError)
// if !filtered {
// 	filtered = sp.maybeRateLimit(fingerprint, filtered, filteredReason)
// }
