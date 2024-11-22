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

package aggregationprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/ottl"
)

func (e *aggregationProcessor) emit(now time.Time) {
	if now.Sub(e.lastEmitCheck) < time.Duration(e.aggregationInterval)*time.Second {
		return
	}
	e.lastEmitCheck = now
	mi := e.aggregatorI.Emit(now)
	for _, set := range mi {
		e.emitSetI(set)
	}
	mf := e.aggregatorF.Emit(now)
	for _, set := range mf {
		e.emitSetF(set)
	}
}

func (e *aggregationProcessor) emitSetI(set *ottl.AggregationSet[int64]) {
	for _, agg := range set.Aggregations {
		mmetrics := pmetric.NewMetrics()
		res := mmetrics.ResourceMetrics().AppendEmpty()
		sm := res.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(agg.Name())

		var dp pmetric.NumberDataPoint
		if agg.AggregationType() == ottl.AggregationTypeSum {
			m.SetEmptySum()
			dp = m.Sum().DataPoints().AppendEmpty()
			m.Sum().SetIsMonotonic(false)
			m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		} else {
			m.SetEmptyGauge()
			dp = m.Gauge().DataPoints().AppendEmpty()
		}
		tstime := time.UnixMilli(set.StartTime)
		ts := pcommon.Timestamp(pcommon.NewTimestampFromTime(tstime))
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetIntValue(agg.Value()[0])

		setTags(res, sm, m, dp, agg.Tags())

		for k, v := range e.config.AdditionalAttributes {
			dp.Attributes().PutStr(k, v)
		}

		err := e.nextMetricReceiver.ConsumeMetrics(context.Background(), mmetrics)
		if err != nil {
			e.logger.Error("Error emitting metrics", zap.Error(err))
		}
	}
}

func (e *aggregationProcessor) emitSetF(set *ottl.AggregationSet[float64]) {
	for _, agg := range set.Aggregations {
		mmetrics := pmetric.NewMetrics()
		res := mmetrics.ResourceMetrics().AppendEmpty()
		sm := res.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(agg.Name())

		var dp pmetric.NumberDataPoint
		if agg.AggregationType() == ottl.AggregationTypeSum {
			m.SetEmptySum()
			dp = m.Sum().DataPoints().AppendEmpty()
			m.Sum().SetIsMonotonic(false)
			m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		} else {
			m.SetEmptyGauge()
			dp = m.Gauge().DataPoints().AppendEmpty()
		}
		tstime := time.UnixMilli(set.StartTime)
		ts := pcommon.Timestamp(pcommon.NewTimestampFromTime(tstime))
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetDoubleValue(agg.Value()[0])

		setTags(res, sm, m, dp, agg.Tags())

		for k, v := range e.config.AdditionalAttributes {
			dp.Attributes().PutStr(k, v)
		}

		err := e.nextMetricReceiver.ConsumeMetrics(context.Background(), mmetrics)
		if err != nil {
			e.logger.Error("Error emitting metrics", zap.Error(err))
		}
		e.logger.Debug("Emitted metrics", zap.Time("timestamp", ts.AsTime()), zap.String("name", agg.Name()), zap.Float64("value", agg.Value()[0]), zap.String("tags", fmt.Sprintf("%v", agg.Tags())))
	}
}

func setTags(res pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint, tags map[string]string) {
	for k, v := range tags {
		section, tagname := ottl.SplitTag(k)
		switch section {
		case "resource":
			res.Resource().Attributes().PutStr(tagname, v)
		case "instrumentation":
			sm.Scope().Attributes().PutStr(tagname, v)
		case "metric":
			dp.Attributes().PutStr(tagname, v)
		case "metadata":
			setMetadata(res, sm, metric, tagname, v)
		}
	}
}

func setMetadata(res pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, tagname string, v string) {
	area, mname := ottl.SplitTag(tagname)
	switch area {
	case "resource":
		setResourceMetadata(res, mname, v)
	case "instrumentation":
		setInstrumentationMetadata(sm, mname, v)
	case "metric":
		setMetricMetadata(metric, mname, v)
	}
}

func setResourceMetadata(res pmetric.ResourceMetrics, tagname string, v string) {
	switch tagname {
	case "schemaurl":
		res.SetSchemaUrl(v)
	}
}

func setInstrumentationMetadata(sm pmetric.ScopeMetrics, tagname string, v string) {
	switch tagname {
	case "schemaurl":
		sm.SetSchemaUrl(v)
	case "version":
		sm.Scope().SetVersion(v)
	case "name":
		sm.Scope().SetName(v)
	}
}

func setMetricMetadata(metric pmetric.Metric, tagname string, v string) {
	switch tagname {
	case "name":
		metric.SetName(v)
	case "description":
		metric.SetDescription(v)
	case "unit":
		metric.SetUnit(v)
	case "aggregationtemporality":
		if metric.Type() != pmetric.MetricTypeSum {
			return
		}
		switch v {
		case "cumulative":
			metric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		case "delta":
			metric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		}
	case "ismonotonic":
		if metric.Type() != pmetric.MetricTypeSum {
			return
		}
		metric.Sum().SetIsMonotonic(v == "true")
	}
}

func (e *aggregationProcessor) aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return e.aggregateGaugeDatapoint(rms, ils, metric, dp)
	case pmetric.MetricTypeSum:
		return e.aggregateSumDatapoint(rms, ils, metric, dp)
	default:
		return false
	}
}

func (e *aggregationProcessor) aggregateGaugeDatapoint(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	metadata := map[string]string{
		"resource.schemaurl":        rms.SchemaUrl(),
		"instrumentation.schemaurl": ils.SchemaUrl(),
		"instrumentation.name":      ils.Scope().Name(),
		"instrumentation.version":   ils.Scope().Version(),
		"metric.name":               metric.Name(),
		"metric.description":        metric.Description(),
		"metric.unit":               metric.Unit(),
	}
	return e.aggregateDatapoint(ottl.AggregationTypeAvg, rms, ils, metric, dp, metadata)
}

func (e *aggregationProcessor) aggregateSumDatapoint(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	metadata := map[string]string{
		"resource.schemaurl":            rms.SchemaUrl(),
		"instrumentation.schemaurl":     ils.SchemaUrl(),
		"instrumentation.name":          ils.Scope().Name(),
		"instrumentation.version":       ils.Scope().Version(),
		"metric.name":                   metric.Name(),
		"metric.description":            metric.Description(),
		"metric.aggregationtemporality": metric.Sum().AggregationTemporality().String(),
		"metric.ismonotonic":            fmt.Sprintf("%t", metric.Sum().IsMonotonic()),
		"metric.unit":                   metric.Unit(),
	}
	return e.aggregateDatapoint(ottl.AggregationTypeSum, rms, ils, metric, dp, metadata)
}

func (e *aggregationProcessor) aggregateDatapoint(
	ty ottl.AggregationType,
	rms pmetric.ResourceMetrics,
	ils pmetric.ScopeMetrics,
	metric pmetric.Metric,
	dp pmetric.NumberDataPoint,
	metadata map[string]string,
) bool {
	t := dp.Timestamp().AsTime()
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v := dp.IntValue()
		matched, err := e.aggregatorI.MatchAndAdd(
			e.logger,
			&t,
			[]int64{1},
			[]int64{v},
			ty,
			metric.Name(),
			metadata,
			rms.Resource().Attributes(),
			ils.Scope().Attributes(),
			dp.Attributes())
		if err != nil {
			e.logger.Error("Error matching and adding int datapoint", zap.Error(err))
			return false
		}
		return matched

	case pmetric.NumberDataPointValueTypeDouble:
		v := dp.DoubleValue()
		matched, err := e.aggregatorF.MatchAndAdd(
			e.logger,
			&t,
			[]float64{1},
			[]float64{v},
			ty,
			metric.Name(),
			metadata,
			rms.Resource().Attributes(),
			ils.Scope().Attributes(),
			dp.Attributes())
		if err != nil {
			e.logger.Error("Error matching and adding float64 datapoint", zap.Error(err))
			return false
		}
		return matched
	default:
		return false
	}
}
