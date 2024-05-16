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
	"net/http"
	"sync"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/spantagger"
)

type spansProcessor struct {
	telemetry *processorTelemetry
	logger    *zap.Logger
	graphURL  string
	apiKey    string
}

func newSpansProcessor(set processor.CreateSettings, config *Config) (*spansProcessor, error) {
	var err error
	sp := &spansProcessor{
		logger:   set.Logger,
		graphURL: config.GraphURL,
		apiKey:   config.APIKey,
	}

	dpt, err := newProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	sp.telemetry = dpt

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

type fingerprintTracker struct {
	sync.Mutex
	fingerprints map[uint64]struct{}
}

var sentFingerprints = fingerprintTracker{
	fingerprints: make(map[uint64]struct{}),
}

func newTrace(fingerprint uint64) bool {
	sentFingerprints.Lock()
	defer sentFingerprints.Unlock()
	if _, ok := sentFingerprints.fingerprints[fingerprint]; ok {
		return false
	}
	sentFingerprints.fingerprints[fingerprint] = struct{}{}
	return true
}

func deleteTrace(fingerprint uint64) {
	sentFingerprints.Lock()
	defer sentFingerprints.Unlock()
	delete(sentFingerprints.fingerprints, fingerprint)
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
	if !newTrace(fingerprint) {
		return nil
	}
	graph, _, err := spantagger.BuildTree(td, int64(fingerprint))
	if err != nil {
		deleteTrace(fingerprint)
		return fmt.Errorf("failed to build graph: %w", err)
	}
	if err := sp.sendGraph(ctx, graph); err != nil {
		deleteTrace(fingerprint)
		return fmt.Errorf("failed to send graph: %w", err)
	}

	return nil
}

func (sp *spansProcessor) decorateTraces(td ptrace.Traces, fingerprint uint64, hasError bool, fpError string) (ptrace.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				span.Attributes().PutInt("_cardinalhq.fingerprint", int64(fingerprint))
				span.Attributes().PutBool("_cardinalhq.trace_has_error", hasError)
				if fpError != "" {
					span.Attributes().PutStr("_cardinalhq.fingerprint_error", fpError)
				}
				if span.ParentSpanID().IsEmpty() {
					span.Attributes().PutBool("_cardinalhq.is_root_span", true)
				}
			}
		}
	}
	return td, nil
}

func (sp *spansProcessor) Shutdown(context.Context) error {
	return nil
}

func (sp *spansProcessor) sendGraph(ctx context.Context, graph *spantagger.Graph) error {
	if sp.graphURL == "" {
		return nil
	}
	b, err := json.Marshal(graph)
	if err != nil {
		return fmt.Errorf("failed to marshal graph: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sp.graphURL, bytes.NewReader(b))
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

	sp.logger.Info("sent graph", zap.String("url", sp.graphURL), zap.Int("status", resp.StatusCode))
	sp.telemetry.record(triggerGraphPosted, 1)
	return nil
}
