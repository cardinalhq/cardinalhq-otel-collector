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

package extractmetricsprocessor

import (
	"context"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/telemetry"
	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/translate"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/ottl"
)

func (e *extractor) ConsumeTraces(ctx context.Context, pt ptrace.Traces) (ptrace.Traces, error) {
	metrics := e.extractMetricsFromSpans(ctx, pt)
	for _, metric := range metrics {
		e.sendMetrics(ctx, e.config.Route, metric)

	}
	return pt, nil
}

func (e *extractor) extractMetricsFromSpans(ctx context.Context, pt ptrace.Traces) []pmetric.Metrics {
	var totalMetrics = []pmetric.Metrics{}

	extractors := e.spanExtractors.Load()
	if extractors == nil {
		return totalMetrics
	}
	for _, spanExtractor := range *extractors {
		metrics := pmetric.NewMetrics()

		resourceSpans := pt.ResourceSpans()
		for i := 0; i < resourceSpans.Len(); i++ {
			resourceSpan := resourceSpans.At(i)
			scopeSpans := resourceSpan.ScopeSpans()
			resource := resourceSpan.Resource()

			resourceMetrics := pmetric.NewResourceMetrics()
			resource.Attributes().CopyTo(resourceMetrics.Resource().Attributes())
			scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
			scopeMetrics.Scope().SetName(componentType.String())

			newMetric := scopeMetrics.Metrics().AppendEmpty()

			newMetric.SetName(spanExtractor.MetricName)
			newMetric.SetUnit(spanExtractor.MetricUnit)

			var dpSlice = pmetric.NewNumberDataPointSlice()
			switch spanExtractor.MetricType {
			case gaugeDoubleType, gaugeIntType:
				dpSlice = newMetric.SetEmptyGauge().DataPoints()
			case counterDoubleType, counterIntType:
				sum := newMetric.SetEmptySum()
				dpSlice = sum.DataPoints()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				sum.SetIsMonotonic(false)
			}

			for j := 0; j < scopeSpans.Len(); j++ {
				scopeSpan := scopeSpans.At(j)
				logRecords := scopeSpan.Spans()
				for k := 0; k < logRecords.Len(); k++ {
					sr := logRecords.At(k)
					spanCtx := ottlspan.NewTransformContext(sr, scopeSpan.Scope(), resource, scopeSpan, resourceSpan)

					matches, err := spanExtractor.EvalSpanConditions(ctx, spanCtx)
					if err != nil {
						e.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
						continue
					}

					if !matches {
						attrset := attribute.NewSet(attribute.String("ruleId", spanExtractor.RuleID), attribute.String("metricName", spanExtractor.MetricName), attribute.String("metricType", spanExtractor.MetricType), attribute.Bool("conditionsEvaluated", false))
						telemetry.CounterAdd(e.rulesEvaluated, 1, metric.WithAttributeSet(attrset))
						continue
					}

					e.spanRecordToDataPoint(ctx, spanExtractor, sr, spanCtx, dpSlice)
				}
			}

			if dpSlice.Len() != 0 {
				// Add the resource metric to the slice if we had any datapoints.
				resourceMetrics.MoveTo(metrics.ResourceMetrics().AppendEmpty())
			}
		}
		if metrics.ResourceMetrics().Len() > 0 {
			totalMetrics = append(totalMetrics, metrics)
		}
	}
	return totalMetrics
}

func (e *extractor) spanRecordToDataPoint(ctx context.Context, se *ottl.SpanExtractor, sr ptrace.Span, spanCtx ottlspan.TransformContext, dpSlice pmetric.NumberDataPointSlice) {
	var val any

	if se.MetricValue != nil {
		computedValue, _, err := se.MetricValue.Execute(ctx, spanCtx)
		if err != nil {
			e.logger.Error("Failed when extracting value.", zap.Error(err))
			return
		}
		val = computedValue
	} else {
		val = 1.0
	}

	attrs := se.ExtractAttributes(ctx, spanCtx)

	dp := pmetric.NewNumberDataPoint()
	err := dp.Attributes().FromRaw(attrs)

	if err != nil {
		e.logger.Error("Failed when setting attributes.", zap.Error(err))
		return
	}

	dp.SetTimestamp(extractTimestampFromSpanRecord(sr))
	switch se.MetricType {
	case gaugeDoubleType, counterDoubleType:
		floatVal, err := convertAnyToFloat(val)
		if err != nil {
			e.logger.Error("Failed when parsing float.", zap.Error(err))
			return
		}

		dp.SetDoubleValue(floatVal)
	case gaugeIntType, counterIntType:
		intVal, err := convertAnyToInt(val)
		if err != nil {
			e.logger.Error("Failed when parsing integer.", zap.Error(err))
			return
		}

		dp.SetIntValue(intVal)
	}

	attrset := attribute.NewSet(attribute.Bool("metricValueExtracted", val != 1),
		attribute.Bool("attributesExtracted", len(attrs) > 0),
		attribute.String("ruleId", se.RuleID),
		attribute.String("metricName", se.MetricName),
		attribute.String("metricType", se.MetricType),
		attribute.Bool("conditionsEvaluated", true))

	telemetry.CounterAdd(e.rulesEvaluated, 1, metric.WithAttributeSet(attrset))

	// Mark this datapoint for aggregation
	dp.Attributes().PutBool(translate.CardinalFieldAggregate, true)

	// Successfully constructed dp, we can add it to the slice
	dp.MoveTo(dpSlice.AppendEmpty())
}

func extractTimestampFromSpanRecord(lr ptrace.Span) pcommon.Timestamp {
	if ts := lr.StartTimestamp(); ts != 0 {
		return ts
	}

	if ts := lr.EndTimestamp(); ts != 0 {
		return ts
	}

	return pcommon.NewTimestampFromTime(time.Now())
}
