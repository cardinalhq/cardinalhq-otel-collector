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

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *extractor) ConsumeLogs(ctx context.Context, pl plog.Logs) (plog.Logs, error) {
	metrics := e.extractMetricsFromLogs(ctx, pl)
	if len(metrics) > 0 {
		for _, metric := range metrics {
			e.sendMetrics(ctx, e.config.Route, metric)
		}
	}
	return pl, nil
}

func (e *extractor) extractMetricsFromLogs(ctx context.Context, pl plog.Logs) []pmetric.Metrics {
	var totalMetrics = []pmetric.Metrics{}

	if e.logExtractors != nil && len(e.logExtractors) > 0 {
		for _, logExtractor := range e.logExtractors {
			metrics := pmetric.NewMetrics()

			resourceLogs := pl.ResourceLogs()
			for i := 0; i < resourceLogs.Len(); i++ {
				resourceLog := resourceLogs.At(i)
				scopeLogs := resourceLog.ScopeLogs()
				resource := resourceLog.Resource()

				resourceMetrics := pmetric.NewResourceMetrics()
				resource.Attributes().CopyTo(resourceMetrics.Resource().Attributes())
				scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
				scopeMetrics.Scope().SetName(componentType.String())

				metric := scopeMetrics.Metrics().AppendEmpty()

				metric.SetName(logExtractor.MetricName)
				metric.SetUnit(logExtractor.MetricUnit)

				var dpSlice = pmetric.NewNumberDataPointSlice()
				switch logExtractor.MetricType {
				case gaugeDoubleType, gaugeIntType:
					dpSlice = metric.SetEmptyGauge().DataPoints()
				case counterDoubleType, counterIntType:
					dpSlice = metric.SetEmptySum().DataPoints()
				}

				for j := 0; j < scopeLogs.Len(); j++ {
					scopeLog := scopeLogs.At(j)
					logRecords := scopeLog.LogRecords()
					for k := 0; k < logRecords.Len(); k++ {
						lr := logRecords.At(k)
						logCtx := ottllog.NewTransformContext(lr, scopeLog.Scope(), resource, scopeLog, resourceLog)

						matches, err := logExtractor.EvalLogConditions(ctx, logCtx)
						if err != nil {
							e.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
							continue
						}

						if !matches {
							continue
						}

						e.logRecordToDataPoint(ctx, logExtractor, lr, logCtx, dpSlice)
					}
				}

				if dpSlice.Len() != 0 {
					// Add the resource metric to the slice if we had any datapoints.
					resourceMetrics.MoveTo(metrics.ResourceMetrics().AppendEmpty())
				}
			}
			totalMetrics = append(totalMetrics, metrics)
		}
	}
	return totalMetrics
}

func (e *extractor) logRecordToDataPoint(ctx context.Context, lex ottl.LogExtractor, lr plog.LogRecord, logCtx ottllog.TransformContext, dpSlice pmetric.NumberDataPointSlice) {
	var val any

	if lex.MetricValue != nil {
		computedValue, _, err := lex.MetricValue.Execute(ctx, logCtx)
		if err != nil {
			e.logger.Error("Failed when extracting value.", zap.Error(err))
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
		e.logger.Error("Failed when setting attributes.", zap.Error(err))
		return
	}

	dp.SetTimestamp(extractTimestampFromLogRecord(lr))
	switch lex.MetricType {
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
