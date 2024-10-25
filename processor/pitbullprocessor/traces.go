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

package pitbullprocessor

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *pitbull) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	e.Lock()
	defer e.Unlock()

	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	// TODO add a short-circuit at each level to skip processing if no transformations are defined for that level.

	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		transformCtx := ottlresource.NewTransformContext(rs.Resource(), rs)
		e.traceTransformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx)
		if _, found := rs.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
			return true
		}
		rs.ScopeSpans().RemoveIf(func(iss ptrace.ScopeSpans) bool {
			transformCtx := ottlscope.NewTransformContext(iss.Scope(), rs.Resource(), rs)
			e.traceTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx)
			if _, found := iss.Scope().Attributes().Get(translate.CardinalFieldDropMarker); found {
				return true
			}
			iss.Spans().RemoveIf(func(sr ptrace.Span) bool {
				transformCtx := ottlspan.NewTransformContext(sr, iss.Scope(), rs.Resource(), iss, rs)
				if len(e.tracesLookupConfigs) > 0 {
					for _, lookupConfig := range e.tracesLookupConfigs {
						lookupConfig.ExecuteSpansRules(context.Background(), transformCtx, sr)
					}
				}
				e.traceTransformations.ExecuteSpanTransforms(e.ottlProcessed, transformCtx)
				_, found := sr.Attributes().Get(translate.CardinalFieldDropMarker)
				return found
			})
			return iss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})

	if td.ResourceSpans().Len() == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}

	return td, nil
}

func (e *pitbull) updateTraceTransformations(sc ottl.ControlPlaneConfig, logger *zap.Logger) {
	e.Lock()
	defer e.Unlock()
	e.logger.Info("Updating trace transformations", zap.Int("num_decorators", len(sc.Spans.Decorators)))
	newTransformations := ottl.NewTransformations(e.logger)

	for _, decorator := range sc.Spans.Decorators {
		if decorator.ProcessorID != e.id.String() {
			continue
		}
		transformations, err := ottl.ParseTransformations(decorator, e.logger)
		if err != nil {
			e.logger.Error("Error parsing traces transformation", zap.Error(err))
			continue
		}
		newTransformations = ottl.MergeWith(newTransformations, transformations)
	}

	oldTransformation := e.traceTransformations
	e.traceTransformations = newTransformations
	oldTransformation.Stop()

	if len(sc.TracesLookupConfigs) > 0 {
		for _, lookupConfig := range sc.TracesLookupConfigs {
			lookupConfig.Init(logger)
		}
	}
}
