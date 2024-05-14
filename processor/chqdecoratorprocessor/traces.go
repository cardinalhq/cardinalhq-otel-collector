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
	"fmt"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/spantagger"
)

type spansProcessor struct {
	telemetry *processorTelemetry
	logger    *zap.Logger
}

func newSpansProcessor(set processor.CreateSettings, _ *Config) (*spansProcessor, error) {
	var err error
	sp := &spansProcessor{
		logger: set.Logger,
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

func fingerprint(traces ptrace.Traces) (uint64, bool, string) {
	fp, he, err := spantagger.Fingerprint(traces)
	switch err {
	case nil:
		return fp, he, ""
	case spantagger.InconsistentTraceIDsError:
		return 0, true, "InconsistentTraceIDs"
	case spantagger.OrphanedSpanError:
		return 0, true, "OrphanedSpan"
	case spantagger.NoRootError:
		return 0, true, "NoRoot"
	case spantagger.MultipleRootsError:
		return 0, true, "MultipleRoots"
	default:
		return 0, true, "UnknownError"
	}
}

func (sp *spansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	fingerprint, hasError, fpError := fingerprint(td)

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
