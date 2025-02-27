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
	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/ottl"
)

func (p *extractor) ConsumeLogs(ctx context.Context, pl plog.Logs) (plog.Logs, error) {
	metrics := p.extractMetricsFromLogs(ctx, pl)
	for _, metricToSend := range metrics {
		p.sendMetrics(ctx, p.config.Route, metricToSend)
	}
	return pl, nil
}

func (p *extractor) extractMetricsFromLogs(ctx context.Context, pl plog.Logs) []pmetric.Metrics {
	var totalMetrics []pmetric.Metrics

	metrics := pmetric.NewMetrics()

	resourceLogs := pl.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		resourceLog := resourceLogs.At(i)
		scopeLogs := resourceLog.ScopeLogs()
		resource := resourceLog.Resource()
		cid := OrgIdFromResource(resource.Attributes())
		logExtractors, ok := p.logExtractors.Load(cid)
		if !ok {
			continue
		}

		for _, logExtractor := range logExtractors {
			startTime := time.Now()
			resourceMetrics := pmetric.NewResourceMetrics()
			// Copy the resource attributes to the resource metrics, which will include
			// the tenant ID we found.
			resource.Attributes().CopyTo(resourceMetrics.Resource().Attributes())
			scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
			scopeMetrics.Scope().SetName(componentType.String())

			attrset := attribute.NewSet(
				attribute.String("processor", p.id.String()),
				attribute.String("signal", p.ttype),
				attribute.String("ruleId", logExtractor.RuleID),
				attribute.String("metricName", logExtractor.MetricName),
				attribute.String("metricType", logExtractor.MetricType),
				attribute.String("organization_id", cid),
			)

			newMetric := scopeMetrics.Metrics().AppendEmpty()

			newMetric.SetName(logExtractor.MetricName)
			newMetric.SetUnit(logExtractor.MetricUnit)

			var dpSlice = pmetric.NewNumberDataPointSlice()
			switch logExtractor.MetricType {
			case gaugeDoubleType, gaugeIntType:
				dpSlice = newMetric.SetEmptyGauge().DataPoints()
			case counterDoubleType, counterIntType:
				sum := newMetric.SetEmptySum()
				dpSlice = sum.DataPoints()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				sum.SetIsMonotonic(false)
			}

			for j := 0; j < scopeLogs.Len(); j++ {
				scopeLog := scopeLogs.At(j)
				logRecords := scopeLog.LogRecords()
				for k := 0; k < logRecords.Len(); k++ {
					lr := logRecords.At(k)
					logCtx := ottllog.NewTransformContext(lr, scopeLog.Scope(), resource, scopeLog, resourceLog)

					matches, err := logExtractor.EvalLogConditions(ctx, logCtx)
					if err != nil {
						p.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}
					telemetry.CounterAdd(p.rulesEvaluated, 1, metric.WithAttributeSet(attrset))
					if !matches {
						continue
					}
					telemetry.CounterAdd(p.rulesExecuted, 1, metric.WithAttributeSet(attrset))

					p.logRecordToDataPoint(ctx, cid, logExtractor, lr, logCtx, dpSlice)
				}
			}

			if dpSlice.Len() != 0 {
				// Add the resource metric to the slice if we had any datapoints.
				resourceMetrics.MoveTo(metrics.ResourceMetrics().AppendEmpty())
			}

			telemetry.HistogramRecord(p.ruleEvalTime, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset))
		}
		if metrics.ResourceMetrics().Len() > 0 {
			totalMetrics = append(totalMetrics, metrics)
		}
	}

	return totalMetrics
}

func (p *extractor) logRecordToDataPoint(ctx context.Context, cid string, lex *ottl.LogExtractor, lr plog.LogRecord, logCtx ottllog.TransformContext, dpSlice pmetric.NumberDataPointSlice) {
	var val any

	if lex.MetricValue != nil {
		computedValue, _, err := lex.MetricValue.Execute(ctx, logCtx)
		if err != nil {
			p.logger.Error("Failed when extracting value.", zap.Error(err))
			attrset := attribute.NewSet(attribute.String("ruleId", lex.RuleID),
				attribute.String("metricName", lex.MetricName),
				attribute.String("metricType", lex.MetricType),
				attribute.String("stage", "metricValueExtraction"),
				attribute.String("error", err.Error()),
				attribute.String("organization_id", cid),
			)

			telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
			return
		}
		val = computedValue
	} else {
		val = 1
	}

	attrs := lex.ExtractAttributes(ctx, logCtx)

	dp := pmetric.NewNumberDataPoint()
	err := dp.Attributes().FromRaw(attrs)

	if err != nil {
		p.logger.Error("Failed when setting attributes.", zap.Error(err))
		attrset := attribute.NewSet(attribute.String("ruleId", lex.RuleID),
			attribute.String("metricName", lex.MetricName),
			attribute.String("metricType", lex.MetricType),
			attribute.String("stage", "attributeExtraction"),
			attribute.String("error_msg", err.Error()))
		telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
		return
	}

	dp.SetTimestamp(extractTimestampFromLogRecord(lr))
	switch lex.MetricType {
	case gaugeDoubleType, counterDoubleType:
		floatVal, err := convertAnyToFloat(val)
		if err != nil {
			p.logger.Error("Failed when parsing float.", zap.Error(err))
			return
		}

		dp.SetDoubleValue(floatVal)
	case gaugeIntType, counterIntType:
		intVal, err := convertAnyToInt(val)
		if err != nil {
			p.logger.Error("Failed when parsing integer.", zap.Error(err))
			return
		}

		dp.SetIntValue(intVal)
	}
	attrset := attribute.NewSet(attribute.Bool("metricValueExtracted", val != 1),
		attribute.Bool("attributesExtracted", len(attrs) > 0),
		attribute.String("metricName", lex.MetricName),
		attribute.String("ruleId", lex.RuleID),
		attribute.String("metricType", lex.MetricType),
		attribute.Bool("conditionsEvaluated", true))
	telemetry.CounterAdd(p.rulesEvaluated, 1, metric.WithAttributeSet(attrset))

	// Mark this datapoint for aggregation
	dp.Attributes().PutBool(translate.CardinalFieldAggregate, true)

	// Successfully constructed dp, we can add it to the slice
	dp.MoveTo(dpSlice.AppendEmpty())
}

func extractTimestampFromLogRecord(lr plog.LogRecord) pcommon.Timestamp {
	if ts := lr.Timestamp(); ts != 0 {
		return ts
	}

	if ts := lr.ObservedTimestamp(); ts != 0 {
		return ts
	}

	return pcommon.NewTimestampFromTime(time.Now())
}
