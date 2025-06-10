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

package extractmetricsprocessor

import (
	"context"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"time"

	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func (p *extractor) ConsumeTraces(ctx context.Context, pt ptrace.Traces) (ptrace.Traces, error) {
	p.updateSketchCache(ctx, pt)
	return pt, nil
}

func (p *extractor) updateSketchCache(ctx context.Context, pl ptrace.Traces) {
	resourceSpans := pl.ResourceSpans()
	for i := range resourceSpans.Len() {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		cid := OrgIdFromResource(resource.Attributes())
		spanExtractors, ok := p.spanExtractors.Load(cid)
		if !ok {
			continue
		}

		sketchCache, sok := p.spanSketchCaches.Load(cid)
		if !sok {
			p.logger.Info("Creating new span sketch cache", zap.String("cid", cid))
			sketchCache = chqpb.NewSpanSketchCache(5*time.Minute, cid, func(list *chqpb.SpanSketchList) error {
				send := p.sendProto("/api/v1/spanSketches", list)
				return send()
			})
			p.spanSketchCaches.Store(cid, sketchCache)
		}

		spanLineSketchCache, lsok := p.spanLineSketchCaches.Load(cid)
		if !lsok {
			p.logger.Info("Creating new span line sketch cache", zap.String("cid", cid))
			spanLineSketchCache = chqpb.NewGenericSketchCache(5*time.Minute, cid, "traces", func(list *chqpb.GenericSketchList) error {
				send := p.sendProto("/api/v1/spanLineSketches", list)
				return send()
			})
			p.spanSketchCaches.Store(cid, sketchCache)
		}

		for j := range resourceSpans.At(i).ScopeSpans().Len() {
			scopeSpan := resourceSpans.At(i).ScopeSpans().At(j)
			for k := range resourceSpans.At(i).ScopeSpans().At(j).Spans().Len() {
				lr := resourceSpans.At(i).ScopeSpans().At(j).Spans().At(k)
				tc := ottlspan.NewTransformContext(lr, scopeSpan.Scope(), resourceSpan.Resource(), scopeSpan, resourceSpan)

				for _, lex := range spanExtractors {
					attrset := attribute.NewSet(
						attribute.String("processor", p.id.String()),
						attribute.String("signal", p.ttype),
						attribute.String("ruleId", lex.RuleID),
						attribute.String("metricName", lex.MetricName),
						attribute.String("metricType", lex.MetricType),
						attribute.String("organization_id", cid),
					)

					matches, err := lex.EvalSpanConditions(ctx, tc)
					if err != nil {
						p.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}
					if !matches {
						continue
					}

					telemetry.CounterAdd(p.rulesEvaluated, 1, metric.WithAttributeSet(attrset))

					extractedVal, err := p.extractSpanValue(ctx, tc, lex)
					var valueToUse float64
					if err == nil && extractedVal != -1 {
						valueToUse = extractedVal
					} else if v, ok := lr.Attributes().Get(translate.CardinalFieldSpanDuration); ok {
						valueToUse = v.Double()
					}

					var parentTID int64
					if len(lex.AggregateDimensions) > 0 {
						mapAttrs := lex.ExtractAggregateAttributes(ctx, tc)
						tags := p.withServiceClusterNamespace(resource, mapAttrs)
						parentTID = sketchCache.Update(lex.MetricName, tags, lr, resource)
					}

					if len(lex.LineDimensions) > 0 {
						mapAttrsByTagFamilyId := lex.ExtractLineAttributes(ctx, tc)
						for tagFamilyId, mapAttrs := range mapAttrsByTagFamilyId {
							tags := p.withServiceClusterNamespace(resource, mapAttrs)
							ts := lr.EndTimestamp().AsTime()
							spanLineSketchCache.Update(lex.MetricName, lex.MetricType, tags, parentTID, tagFamilyId, valueToUse, ts)
						}
					}
				}
			}
		}
	}
}

func (p *extractor) extractSpanValue(ctx context.Context, tc ottlspan.TransformContext, e *ottl.SpanExtractor) (float64, error) {
	if e.MetricValue != nil {
		val, _, err := e.MetricValue.Execute(ctx, tc)
		if err != nil {
			return 0, err
		}
		return convertAnyToFloat(val)
	}
	return -1, nil
}
