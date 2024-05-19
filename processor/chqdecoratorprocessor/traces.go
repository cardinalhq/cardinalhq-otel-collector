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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/spantagger"
	"github.com/hashicorp/go-multierror"
	"github.com/honeycombio/dynsampler-go"
)

type spansProcessor struct {
	telemetry   *processorTelemetry
	logger      *zap.Logger
	traceConfig *TraceConfig
	apiKey      string

	sketches             map[uint64]*ddsketch.DDSketch
	sentFingerprints     fingerprintTracker
	slowSampler          dynsampler.Sampler
	hasErrorSampler      dynsampler.Sampler
	uninterestingSampler dynsampler.Sampler
}

type fingerprintTracker struct {
	sync.Mutex
	fingerprints map[uint64]struct{}
}

func newSpansProcessor(set processor.CreateSettings, config *Config) (*spansProcessor, error) {
	sp := &spansProcessor{
		logger:      set.Logger,
		traceConfig: &config.TraceConfig,
		apiKey:      config.APIKey,
		sketches:    make(map[uint64]*ddsketch.DDSketch),
		sentFingerprints: fingerprintTracker{
			fingerprints: make(map[uint64]struct{}),
		},
		slowSampler:          &dynsampler.AvgSampleWithMin{GoalSampleRate: *config.TraceConfig.SlowRate},
		hasErrorSampler:      &dynsampler.AvgSampleWithMin{GoalSampleRate: *config.TraceConfig.HasErrorRate},
		uninterestingSampler: &dynsampler.AvgSampleWithMin{GoalSampleRate: *config.TraceConfig.UninterestingRate},
	}

	dpt, err := newProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	sp.telemetry = dpt

	if err := sp.slowSampler.Start(); err != nil {
		return nil, fmt.Errorf("error starting slow sampler: %w", err)
	}
	if err := sp.hasErrorSampler.Start(); err != nil {
		return nil, fmt.Errorf("error starting has error sampler: %w", err)
	}
	if err := sp.uninterestingSampler.Start(); err != nil {
		return nil, fmt.Errorf("error starting uninteresting sampler: %w", err)
	}

	set.Logger.Info(
		"Decorator processor configured",
	)

	return sp, nil
}

func getFingerprint(traces ptrace.Traces) (uint64, bool, string) {
	fp, he, err := spantagger.Fingerprint(traces)
	switch err {
	case nil:
		return fp, he, ""
	case spantagger.InconsistentTraceIDsError:
		return 0, he, "InconsistentTraceIDs"
	case spantagger.OrphanedSpanError:
		return 0, he, "OrphanedSpan"
	case spantagger.NoRootError:
		return 0, he, "NoRoot"
	case spantagger.MultipleRootsError:
		return 0, he, "MultipleRoots"
	default:
		return 0, he, "UnknownError"
	}
}

func (sp *spansProcessor) newTrace(fingerprint uint64) bool {
	sp.sentFingerprints.Lock()
	defer sp.sentFingerprints.Unlock()
	if _, ok := sp.sentFingerprints.fingerprints[fingerprint]; ok {
		return false
	}
	sp.sentFingerprints.fingerprints[fingerprint] = struct{}{}
	return true
}

func (sp *spansProcessor) deleteTrace(fingerprint uint64) {
	sp.sentFingerprints.Lock()
	defer sp.sentFingerprints.Unlock()
	delete(sp.sentFingerprints.fingerprints, fingerprint)
}

func (sp *spansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	fingerprint, hasError, fpError := getFingerprint(td)
	if err := sp.postFingerprint(ctx, td, fingerprint); err != nil {
		sp.logger.Warn("failed to post fingerprint", zap.Error(err))
	}
	return sp.decorateTraces(td, fingerprint, hasError, fpError)
}

func (sp *spansProcessor) postFingerprint(ctx context.Context, td ptrace.Traces, fingerprint uint64) error {
	if fingerprint == 0 {
		return nil
	}
	if !sp.newTrace(fingerprint) {
		return nil
	}
	graph, _, err := spantagger.BuildTree(td, int64(fingerprint))
	if err != nil {
		sp.deleteTrace(fingerprint)
		return fmt.Errorf("failed to build graph: %w", err)
	}
	if err := sp.sendGraph(ctx, graph); err != nil {
		sp.deleteTrace(fingerprint)
		return fmt.Errorf("failed to send graph: %w", err)
	}

	return nil
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

// isSlow returns true if the trace is slow compared to the 75% percentile
// of traces with the same fingerprint.
func (sp *spansProcessor) isSlow(td ptrace.Traces, fingerprint uint64) bool {
	rootDuration, found := findRootDuration(td)
	if !found {
		return false
	}
	return sp.slowPercentile(fingerprint, rootDuration)
}

func (sp *spansProcessor) findSketch(fingerprint uint64) (*ddsketch.DDSketch, error) {
	sketch, ok := sp.sketches[fingerprint]
	if !ok {
		newSketch, err := ddsketch.NewDefaultDDSketch(0.01)
		if err != nil {
			sp.logger.Warn("failed to create sketch", zap.Error(err))
			return nil, err
		}
		sp.sketches[fingerprint] = newSketch
		return newSketch, nil
	}
	return sketch, nil
}

func (sp *spansProcessor) slowPercentile(fingerprint uint64, duration int64) bool {
	sketch, err := sp.findSketch(fingerprint)
	if err != nil {
		return false
	}
	if err := sketch.Add(float64(duration)); err != nil {
		if err != ddsketch.ErrUntrackableTooHigh {
			return true // too large means too large, so it's likely too slow
		}
		sp.logger.Warn("failed to add value to sketch", zap.Error(err))
		return false
	}
	v, err := sketch.GetValueAtQuantile(0.75)
	if err != nil {
		sp.logger.Warn("failed to get value at quantile", zap.Error(err))
		return false
	}
	return float64(duration) > v
}

func (sp *spansProcessor) shouldFilter(td ptrace.Traces, fingerprint uint64, hasError bool) (bool, filteredReason) {
	if fingerprint == 0 {
		return true, filteredReasonInvalid
	}
	slow := sp.isSlow(td, fingerprint) // always call this to update our sketch
	if hasError {
		return false, filteredReasonTraceHasError
	}
	if slow {
		return false, filteredReasonSlow
	}
	return false, filteredReasonUninteresting
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

type filteredReason string

const (
	filteredReasonTraceHasError filteredReason = "trace_has_error"
	filteredReasonSlow          filteredReason = "slow"
	filteredReasonUninteresting filteredReason = "uninteresting"
	filteredReasonInvalid       filteredReason = "invalid_fingerprint"
)

func (sp *spansProcessor) rateLimitSlow(fingerprint uint64) bool {
	rate := sp.slowSampler.GetSampleRate(fmt.Sprintf("%d", fingerprint))
	return rateHelper(rate)
}

func (sp *spansProcessor) rateLimitHasError(fingerprint uint64) bool {
	rate := sp.hasErrorSampler.GetSampleRate(fmt.Sprintf("%d", fingerprint))
	return rateHelper(rate)
}

func (sp *spansProcessor) rateLimitUninteresting(fingerprint uint64) bool {
	rate := sp.uninterestingSampler.GetSampleRate(fmt.Sprintf("%d", fingerprint))
	return rateHelper(rate)
}

func (sp *spansProcessor) maybeRateLimit(fingerprint uint64, filtered bool, filteredReason filteredReason) bool {
	if filtered {
		return filtered
	}

	switch filteredReason {
	case filteredReasonSlow:
		if sp.rateLimitSlow(fingerprint) {
			return true
		}
	case filteredReasonTraceHasError:
		if sp.rateLimitHasError(fingerprint) {
			return true
		}
	case filteredReasonUninteresting:
		if sp.rateLimitUninteresting(fingerprint) {
			return true
		}
	}

	return filtered
}

var processorCounter atomic.Int64

type tracker struct {
	sync.Mutex
	traces map[pcommon.TraceID]struct{}
}

func (t *tracker) newTraceID(traceID pcommon.TraceID) bool {
	t.Lock()
	defer t.Unlock()
	if _, ok := t.traces[traceID]; ok {
		return false
	}
	t.traces[traceID] = struct{}{}
	return true
}

func (sp *spansProcessor) decorateTraces(td ptrace.Traces, fingerprint uint64, hasError bool, fpError string) (ptrace.Traces, error) {
	// First, check to see if this trace is interesting.  If it is not,
	// we will have filtered set to true.  In that case, we only want to
	// rate limit the unfiltered traces.
	filtered, filteredReason := sp.shouldFilter(td, fingerprint, hasError)
	if !filtered {
		filtered = sp.maybeRateLimit(fingerprint, filtered, filteredReason)
	}

	counter := processorCounter.Add(1)

	spancount := int64(0)
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				spancount++
				span := spans.At(k)
				span.Attributes().PutBool("_cardinalhq.filtered", filtered)
				span.Attributes().PutBool("_cardinalhq.would_filter", filtered)
				span.Attributes().PutStr("_cardinalhq.filtered_reason", string(filteredReason))
				span.Attributes().PutInt("_cardinalhq.fingerprint", int64(fingerprint))
				span.Attributes().PutBool("_cardinalhq.trace_has_error", hasError)
				span.Attributes().PutBool("_cardinalhq.span_has_error", span.Status().Code() == ptrace.StatusCodeError)
				if fpError != "" {
					span.Attributes().PutStr("_cardinalhq.fingerprint_error", fpError)
				}
				if span.ParentSpanID().IsEmpty() {
					span.Attributes().PutBool("_cardinalhq.is_root_span", true)
				}
				span.Attributes().PutInt("_cardinalhq.bundle_id", counter)
			}
		}
	}

	attributes := []attribute.KeyValue{
		attribute.Bool("filtered.status", filtered),
		attribute.String("filtered.classification", string(filteredReason)),
		attribute.Int64("filtered.fingerprint", int64(fingerprint)),
	}
	sp.telemetry.record(triggerTracesProcessed, 1, attributes...)
	sp.telemetry.record(triggerSpansProcessed, spancount, attributes...)

	return td, nil
}

func (sp *spansProcessor) Shutdown(context.Context) error {
	var errors *multierror.Error

	errors = multierror.Append(errors, sp.slowSampler.Stop())
	errors = multierror.Append(errors, sp.hasErrorSampler.Stop())
	return errors.ErrorOrNil()
}

func (sp *spansProcessor) sendGraph(ctx context.Context, graph *spantagger.Graph) error {
	u := sp.traceConfig.GraphURL
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
	if sp.apiKey != "" {
		req.Header.Set("x-cardinalhq-api-key", sp.apiKey)
		req.Header.Set("dd-api-key", sp.apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send graph: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send graph: http status %d", resp.StatusCode)
	}

	sp.telemetry.record(triggerGraphPosted, 1)
	return nil
}
