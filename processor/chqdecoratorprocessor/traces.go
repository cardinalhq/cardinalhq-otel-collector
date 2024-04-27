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

func (sp *spansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		//resource := rs.Resource()
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			//scope := ils.Scope()
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				sp.augment(span)
			}
		}
	}
	return td, nil
}

func (sp *spansProcessor) augment(span ptrace.Span) {
	span.Attributes().PutStr("_cardinalhq.was", "here")
}

func (sp *spansProcessor) Shutdown(context.Context) error {
	return nil
}
