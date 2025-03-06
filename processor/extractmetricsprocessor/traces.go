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
	"time"

	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/signalbuilder"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func (p *extractor) ConsumeTraces(ctx context.Context, pt ptrace.Traces) (ptrace.Traces, error) {
	metrics := p.extractMetricsFromTraces(ctx, pt)
	p.sendMetrics(ctx, p.config.Route, metrics)
	return pt, nil
}

func (p *extractor) extractMetricsFromTraces(ctx context.Context, pl ptrace.Traces) pmetric.Metrics {
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	builder := signalbuilder.NewMetricsBuilder()

	resourceSpans := pl.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		cid := OrgIdFromResource(resource.Attributes())
		spanExtractors, ok := p.spanExtractors.Load(cid)
		if !ok {
			continue
		}

		resourceBuilder := builder.Resource(resource.Attributes())
		scopeBuilder := resourceBuilder.Scope(pcommon.NewMap())

		for j := 0; j < resourceSpans.At(i).ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpans.At(i).ScopeSpans().At(j)
			for k := 0; k < resourceSpans.At(i).ScopeSpans().At(j).Spans().Len(); k++ {
				lr := resourceSpans.At(i).ScopeSpans().At(j).Spans().At(k)
				logCtx := ottlspan.NewTransformContext(lr, scopeSpan.Scope(), resourceSpan.Resource(), scopeSpan, resourceSpan)

				for _, spanExtractor := range spanExtractors {
					attrset := attribute.NewSet(
						attribute.String("processor", p.id.String()),
						attribute.String("signal", p.ttype),
						attribute.String("ruleId", spanExtractor.RuleID),
						attribute.String("metricName", spanExtractor.MetricName),
						attribute.String("metricType", spanExtractor.MetricType),
						attribute.String("organization_id", cid),
					)

					matches, err := spanExtractor.EvalSpanConditions(ctx, logCtx)
					if err != nil {
						p.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}

					if !matches {
						continue
					}

					var val any
					if spanExtractor.MetricValue != nil {
						computedVal, _, err := spanExtractor.MetricValue.Execute(ctx, logCtx)
						if err != nil {
							p.logger.Error("Failed when extracting value.", zap.Error(err))
							attrset := attribute.NewSet(attribute.String("ruleId", spanExtractor.RuleID),
								attribute.String("metricName", spanExtractor.MetricName),
								attribute.String("metricType", spanExtractor.MetricType),
								attribute.String("stage", "metricValueExtraction"),
								attribute.String("error", err.Error()),
								attribute.String("organization_id", cid),
							)

							telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
							continue
						}
						telemetry.CounterAdd(p.rulesEvaluated, 1, metric.WithAttributeSet(attrset))
						val = computedVal
					} else {
						val = 1
					}
					fv, err := convertAnyToFloat(val)
					if err != nil {
						p.logger.Error("Failed when converting value to float.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}

					mapAttrs := spanExtractor.ExtractAttributes(ctx, logCtx)
					attrs := pcommon.NewMap()
					if err := attrs.FromRaw(mapAttrs); err != nil {
						p.logger.Error("Failed when extracting attributes.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}

					switch spanExtractor.MetricType {
					case gaugeDoubleType, gaugeIntType:
						metric, err := scopeBuilder.Metric(spanExtractor.MetricName, spanExtractor.MetricUnit, pmetric.MetricTypeGauge)
						if err != nil {
							p.logger.Error("Failed when creating metric.", zap.Error(err))
							continue
						}
						dp, _, isNew := metric.Datapoint(attrs, timestamp)
						if isNew {
							dp.SetDoubleValue(fv)
						} else {
							if dp.DoubleValue() < fv {
								dp.SetDoubleValue(fv)
							}
						}
					case counterDoubleType, counterIntType:
						metric, err := scopeBuilder.Metric(spanExtractor.MetricName, spanExtractor.MetricUnit, pmetric.MetricTypeSum)
						if err != nil {
							p.logger.Error("Failed when creating metric.", zap.Error(err))
							continue
						}
						dp, _, isNew := metric.Datapoint(attrs, timestamp)
						if isNew {
							dp.SetDoubleValue(fv)
						} else {
							dp.SetDoubleValue(dp.DoubleValue() + fv)
						}
					}
				}
			}
		}
	}

	return builder.Build()
}
