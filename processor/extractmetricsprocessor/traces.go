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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/ottl"
)

func (p *extractor) ConsumeTraces(ctx context.Context, pt ptrace.Traces) (ptrace.Traces, error) {
	metrics := p.extractMetricsFromTraces(ctx, pt)
	for _, sendMetric := range metrics {
		p.sendMetrics(ctx, p.config.Route, sendMetric)
	}
	return pt, nil
}

func (p *extractor) extractMetricsFromTraces(ctx context.Context, pl ptrace.Traces) []pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	tagsByFingerprint := make(map[uint64]map[string]string)
	sums := make(map[*ottl.SpanExtractor]map[uint64]float64)
	lastValue := make(map[*ottl.SpanExtractor]map[uint64]float64)

	resourceSpans := pl.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		cid := OrgIdFromResource(resource.Attributes())
		spanExtractors, ok := p.spanExtractors.Load(cid)
		if !ok {
			continue
		}

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

					if matches {
						var val any
						if spanExtractor.MetricValue != nil {
							computedVal, _, err := spanExtractor.MetricValue.Execute(ctx, logCtx)
							if err != nil {
								return nil
							}
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
						attrs := spanExtractor.ExtractAttributes(ctx, logCtx)
						attributesMap := map[string]string{}
						for k, v := range attrs {
							attributesMap[k] = v.(string)
						}
						fingerprint := ottl.FingerprintTags(attributesMap)

						floatVal, err := convertAnyToFloat(val)
						if err != nil {
							p.logger.Error("Failed when parsing float.", zap.Error(err))
							return nil
						}
						if _, ok := sums[spanExtractor]; !ok {
							sums[spanExtractor] = make(map[uint64]float64)
						}
						sums[spanExtractor][fingerprint] += floatVal

						if _, ok := lastValue[spanExtractor]; !ok {
							lastValue[spanExtractor] = make(map[uint64]float64)
						}
						lastValue[spanExtractor][fingerprint] = floatVal
						tagsByFingerprint[fingerprint] = attributesMap
					}
				}
			}
		}

		if len(sums) > 0 {
			resourceMetrics := pmetric.NewResourceMetrics()
			resource.Attributes().CopyTo(resourceMetrics.Resource().Attributes())
			scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
			scopeMetrics.Scope().SetName(componentType.String())

			for lex, sumMap := range sums {
				newMetric := scopeMetrics.Metrics().AppendEmpty()
				newMetric.SetName(lex.MetricName)
				newMetric.SetUnit(lex.MetricUnit)

				var dpSlice = pmetric.NewNumberDataPointSlice()
				var stampLastValue bool
				switch lex.MetricType {
				case gaugeDoubleType, gaugeIntType:
					dpSlice = newMetric.SetEmptyGauge().DataPoints()
					stampLastValue = true
				case counterDoubleType, counterIntType:
					sum := newMetric.SetEmptySum()
					dpSlice = sum.DataPoints()
					sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					sum.SetIsMonotonic(false)
				}

				for fingerprint, sum := range sumMap {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					for k, v := range tagsByFingerprint[fingerprint] {
						dp.Attributes().PutStr(k, v)
					}
					dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					if stampLastValue {
						dp.SetDoubleValue(lastValue[lex][fingerprint])
					} else {
						dp.SetDoubleValue(sum)
					}
					dp.MoveTo(dpSlice.AppendEmpty())
				}
			}
			resourceMetrics.MoveTo(metrics.ResourceMetrics().AppendEmpty())
		}
	}
	return []pmetric.Metrics{metrics}
}
