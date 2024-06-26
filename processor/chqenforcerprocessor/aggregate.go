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

package chqenforcerprocessor

import (
	"context"
	"fmt"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func (e *chqEnforcer) emit() {
	now := time.Now()
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

func (e *chqEnforcer) emitSetI(set *sampler.AggregationSet[int64]) {
	now := time.Now()
	for _, agg := range set.Aggregations {
		mmetrics := pmetric.NewMetrics()
		res := mmetrics.ResourceMetrics().AppendEmpty()
		sm := res.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(agg.Name())

		var dp pmetric.NumberDataPoint
		if agg.AggregationType() == sampler.AggregationTypeSum {
			m.SetEmptySum()
			dp = m.Sum().DataPoints().AppendEmpty()
			m.Sum().SetIsMonotonic(false)
			m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		} else {
			m.SetEmptyGauge()
			dp = m.Gauge().DataPoints().AppendEmpty()
		}
		ts := pcommon.NewTimestampFromTime(now)
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetIntValue(agg.Value()[0])

		setTags(m.Name(), res, sm, m, dp, agg.Tags(), e.podName)
		serviceName := getServiceName(res.Resource().Attributes())
		e.processDatapoint(now, m.Name(), serviceName, res.Resource().Attributes(), sm.Scope().Attributes(), dp.Attributes())
		e.emit()

		err := e.nextMetricReceiver.ConsumeMetrics(context.Background(), mmetrics)
		if err != nil {
			e.logger.Error("Error emitting metrics", zap.Error(err))
		}
	}
}

func (e *chqEnforcer) emitSetF(set *sampler.AggregationSet[float64]) {
	now := time.Now()
	for _, agg := range set.Aggregations {
		mmetrics := pmetric.NewMetrics()
		res := mmetrics.ResourceMetrics().AppendEmpty()
		sm := res.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(agg.Name())

		var dp pmetric.NumberDataPoint
		if agg.AggregationType() == sampler.AggregationTypeSum {
			m.SetEmptySum()
			dp = m.Sum().DataPoints().AppendEmpty()
			m.Sum().SetIsMonotonic(false)
			m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		} else {
			m.SetEmptyGauge()
			dp = m.Gauge().DataPoints().AppendEmpty()
		}
		ts := pcommon.NewTimestampFromTime(now)
		dp.SetTimestamp(ts)
		dp.SetStartTimestamp(ts)
		dp.SetDoubleValue(agg.Value()[0])

		setTags(m.Name(), res, sm, m, dp, agg.Tags(), e.podName)
		serviceName := getServiceName(res.Resource().Attributes())
		e.processDatapoint(now, m.Name(), serviceName, res.Resource().Attributes(), sm.Scope().Attributes(), dp.Attributes())
		e.emit()

		err := e.nextMetricReceiver.ConsumeMetrics(context.Background(), mmetrics)
		if err != nil {
			e.logger.Error("Error emitting metrics", zap.Error(err))
		}
	}
}

func setTags(metricName string, res pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint, tags map[string]string, podName string) {
	for k, v := range tags {
		section, tagname := sampler.SplitTag(k)
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

	dp.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, podName)
	tid := translate.CalculateTID(map[string]string{"name": metricName}, res.Resource().Attributes(), sm.Scope().Attributes(), dp.Attributes(), "metric")
	dp.Attributes().PutInt(translate.CardinalFieldTID, tid)
}

func setMetadata(res pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, tagname string, v string) {
	area, mname := sampler.SplitTag(tagname)
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

func (e *chqEnforcer) aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	if !e.aggregatorF.HasRules() && !e.aggregatorI.HasRules() {
		return false
	}
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return e.aggregateGaugeDatapoint(rms, ils, metric, dp)
	case pmetric.MetricTypeSum:
		return e.aggregateSumDatapoint(rms, ils, metric, dp)
	default:
		return false
	}
}

func (e *chqEnforcer) aggregateGaugeDatapoint(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	metadata := map[string]string{
		"resource.schemaurl":        rms.SchemaUrl(),
		"instrumentation.schemaurl": ils.SchemaUrl(),
		"instrumentation.name":      ils.Scope().Name(),
		"instrumentation.version":   ils.Scope().Version(),
		"metric.name":               metric.Name(),
		"metric.description":        metric.Description(),
		"metric.unit":               metric.Unit(),
	}
	rulematch := e.aggregateDatapoint(sampler.AggregationTypeAvg, rms, ils, metric, dp, metadata)
	return rulematch != nil
}

func (e *chqEnforcer) aggregateSumDatapoint(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
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
	rulematch := e.aggregateDatapoint(sampler.AggregationTypeSum, rms, ils, metric, dp, metadata)
	return rulematch != nil
}

func (e *chqEnforcer) aggregateDatapoint(
	ty sampler.AggregationType,
	rms pmetric.ResourceMetrics,
	ils pmetric.ScopeMetrics,
	metric pmetric.Metric,
	dp pmetric.NumberDataPoint,
	metadata map[string]string,
) *sampler.AggregatorConfigV1 {
	t := dp.Timestamp().AsTime()
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v := dp.IntValue()
		rmatch, err := e.aggregatorI.MatchAndAdd(
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
			return nil
		}
		return rmatch
	case pmetric.NumberDataPointValueTypeDouble:
		v := dp.DoubleValue()
		rmatch, err := e.aggregatorF.MatchAndAdd(&t,
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
			return nil
		}
		return rmatch
	case pmetric.NumberDataPointValueTypeEmpty:
		return nil
	default:
		return nil
	}
}
