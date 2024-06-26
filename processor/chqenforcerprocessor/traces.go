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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

func (e *chqEnforcer) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	//
	// At this stage, coming from the decorator and before being otherwise grouped,
	// each trace bundle is its own thing.  We can pull the various flags out of
	// any of the spans in the bundle, and use them to decide whether to filter
	// the trace bundle.
	//
	fingerprint, hasError := e.getFingerprint(td)
	if fingerprint == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}
	filtered, filteredReason := e.shouldFilter(td, fingerprint, hasError)
	if !filtered {
		filtered = e.maybeRateLimit(fingerprint, filtered, filteredReason)
	}

	if e.config.TraceConfig.GraphURL != "" {
		if err := e.postFingerprint(ctx, td, fingerprint); err != nil {
			e.logger.Warn("failed to post fingerprint", zap.Error(err))
		}
	}

	if filtered {
		return td, processorhelper.ErrSkipProcessingData
	}

	if e.config.DropDecorationAttributes {
		dropDecorations(td)
	}
	return td, nil
}

func dropDecorations(td ptrace.Traces) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				removeAllCardinalFields(span.Attributes())
			}
		}
	}
}

func (e *chqEnforcer) getFingerprint(td ptrace.Traces) (uint64, bool) {
	rs := td.ResourceSpans()
	if rs.Len() == 0 {
		return 0, false
	}
	rs0 := rs.At(0)
	ilss := rs0.ScopeSpans()
	if ilss.Len() == 0 {
		return 0, false
	}
	ils0 := ilss.At(0)
	spans := ils0.Spans()
	if spans.Len() == 0 {
		return 0, false
	}
	span0 := spans.At(0)

	fingerprint := getFingerprint(span0.Attributes())
	if fingerprint == 0 {
		return 0, false
	}

	hasErrorVal, found := span0.Attributes().Get(translate.CardinalFieldTraceHasError)
	if !found {
		return 0, false
	}
	hasError := hasErrorVal.Bool()

	return uint64(fingerprint), hasError
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
