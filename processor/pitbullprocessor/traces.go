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

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (e *pitbull) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	transformations := e.traceTransformations.Load()
	luc := e.tracesLookupConfigs.Load()
	if transformations == nil && luc == nil {
		return td, nil
	}

	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		transformCtx := ottlresource.NewTransformContext(rs.Resource(), rs)
		if transformations != nil {
			transformations.ExecuteResourceTransforms(e.logger, e.ottlProcessed, e.ottlErrors, e.histogram, transformCtx)
			if _, found := rs.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
				return true
			}
		}
		rs.ScopeSpans().RemoveIf(func(iss ptrace.ScopeSpans) bool {
			transformCtx := ottlscope.NewTransformContext(iss.Scope(), rs.Resource(), rs)
			if transformations != nil {
				transformations.ExecuteScopeTransforms(e.logger, e.ottlProcessed, e.ottlErrors, e.histogram, transformCtx)
				if _, found := iss.Scope().Attributes().Get(translate.CardinalFieldDropMarker); found {
					return true
				}
			}
			iss.Spans().RemoveIf(func(sr ptrace.Span) bool {
				transformCtx := ottlspan.NewTransformContext(sr, iss.Scope(), rs.Resource(), iss, rs)
				if luc != nil {
					for _, lookupConfig := range *luc {
						lookupConfig.ExecuteSpansRules(context.Background(), transformCtx, sr)
					}
				}
				if transformations == nil {
					return false
				}
				transformations.ExecuteSpanTransforms(e.logger, e.ottlProcessed, e.ottlErrors, e.histogram, transformCtx)
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

func (e *pitbull) updateTraceTransformations(sc *ottl.PitbullProcessorConfig, logger *zap.Logger) {
	if sc == nil {
		return
	}
	e.logger.Info("Updating trace transformations", zap.Int("num_decorators", len(sc.SpanStatements)))
	newTransformations := ottl.NewTransformations()

	transformations, err := ottl.ParseTransformations(e.logger, sc.SpanStatements)
	if err != nil {
		e.logger.Error("Error parsing traces transformation", zap.Error(err))
	} else {
		newTransformations = ottl.MergeWith(newTransformations, transformations)
	}

	oldTransformations := e.traceTransformations.Load()
	e.traceTransformations.Store(newTransformations)
	if oldTransformations != nil {
		oldTransformations.Stop()
	}

	for _, lookupConfig := range sc.SpanLookupConfigs {
		lookupConfig.Init(logger)
	}
}
