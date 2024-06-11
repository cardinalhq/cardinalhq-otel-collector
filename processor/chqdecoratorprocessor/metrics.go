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

package chqdecoratorprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/sampler"
)

type metricProcessor struct {
	telemetry           *processorTelemetry
	logger              *zap.Logger
	aggregationInterval int64
	nextConsumer        consumer.Metrics
	configManager       sampler.ConfigManager
	updaterId           int
	aggregatorI         sampler.MetricAggregator[int64]
	aggregatorF         sampler.MetricAggregator[float64]
	lastEmitCheck       time.Time
}

func newMetricProcessor(set processor.CreateSettings, conf *Config, nextConsumer consumer.Metrics) (*metricProcessor, error) {
	var err error
	mp := &metricProcessor{
		logger:              set.Logger,
		aggregationInterval: conf.MetricConfig.MetricAggregationInterval,
		nextConsumer:        nextConsumer,
		lastEmitCheck:       time.Now(),
		aggregatorI:         sampler.NewMetricAggregatorImpl[int64](conf.MetricConfig.MetricAggregationInterval, nil),
		aggregatorF:         sampler.NewMetricAggregatorImpl[float64](conf.MetricConfig.MetricAggregationInterval, nil),
	}

	if conf.SamplerConfigFile != "" {
		confmgr, err := makeConfigurationManager(conf, set.Logger)
		if err != nil {
			return nil, fmt.Errorf("error creating configuration manager: %w", err)
		}
		go confmgr.Run()
		mp.configManager = confmgr

		mp.updaterId = confmgr.RegisterCallback("metricsampler", func(config sampler.SamplerConfig) {
			mp.aggregatorI.Configure(config.Metrics.Aggregators)
			mp.aggregatorF.Configure(config.Metrics.Aggregators)
		})
	}

	dpt, err := newProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	mp.telemetry = dpt

	set.Logger.Info(
		"Decorator processor configured",
	)

	return mp, nil
}

func (mp *metricProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	aggregated := int64(0)
	processed := int64(0)
	md.ResourceMetrics().RemoveIf(func(rms pmetric.ResourceMetrics) bool {
		rms.ScopeMetrics().RemoveIf(func(ils pmetric.ScopeMetrics) bool {
			ils.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				processed++
				aggregated = aggregated + mp.aggregate(rms, ils, metric)
				// decorate, don't drop
				return false
			})
			return rms.ScopeMetrics().Len() == 0
		})
		return md.ResourceMetrics().Len() == 0
	})

	mp.telemetry.record(triggerMetricDataPointsAggregated, aggregated)
	mp.telemetry.record(triggerMetricsProcessed, processed-aggregated, attribute.Bool("filtered.filtered", false), attribute.String("filtered.classification", "unaggregated"))
	mp.telemetry.record(triggerMetricsProcessed, aggregated, attribute.Bool("filtered.filtered", true), attribute.String("filtered.classification", "aggregated"))

	mp.emit()

	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

func (mp *metricProcessor) emit() {
	now := time.Now()
	if now.Sub(mp.lastEmitCheck) < time.Duration(mp.aggregationInterval)*time.Second {
		return
	}
	mp.lastEmitCheck = now
	mi := mp.aggregatorI.Emit(now)
	for _, set := range mi {
		mp.emitSetI(set)
	}
	mf := mp.aggregatorF.Emit(now)
	for _, set := range mf {
		mp.emitSetF(set)
	}
}

func (mp *metricProcessor) emitSetI(set *sampler.AggregationSet[int64]) {
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

		setTags(res, sm, m, dp, agg.Tags())

		err := mp.nextConsumer.ConsumeMetrics(context.Background(), mmetrics)
		if err != nil {
			mp.logger.Error("Error emitting metrics", zap.Error(err))
		}
	}
}

func (mp *metricProcessor) emitSetF(set *sampler.AggregationSet[float64]) {
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

		setTags(res, sm, m, dp, agg.Tags())

		err := mp.nextConsumer.ConsumeMetrics(context.Background(), mmetrics)
		if err != nil {
			mp.logger.Error("Error emitting metrics", zap.Error(err))
		}
	}
}

func setTags(res pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint, tags map[string]string) {
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
	dp.Attributes().PutBool(translate.CardinalFieldAggregatedOutput, true)
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

func (mp *metricProcessor) aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) int64 {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return mp.aggregateGauge(rms, ils, metric)
	case pmetric.MetricTypeSum:
		return mp.aggregateSum(rms, ils, metric)
	case pmetric.MetricTypeHistogram:
		return 0
		//return mp.AggregateHistogram(rms, ils, metric)
	case pmetric.MetricTypeExponentialHistogram:
		return 0
	case pmetric.MetricTypeSummary:
		return 0
	default:
		return 0
	}
}

func (mp *metricProcessor) aggregateGauge(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) int64 {
	aggregated := int64(0)
	for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)
		filtered := false
		metadata := map[string]string{
			"resource.schemaurl":        rms.SchemaUrl(),
			"instrumentation.schemaurl": ils.SchemaUrl(),
			"instrumentation.name":      ils.Scope().Name(),
			"instrumentation.version":   ils.Scope().Version(),
			"metric.name":               metric.Name(),
			"metric.description":        metric.Description(),
			"metric.unit":               metric.Unit(),
		}
		rulematch := mp.aggregateDatapoint(sampler.AggregationTypeAvg, rms, ils, metric, dp, metadata)
		if rulematch != nil {
			aggregated++
			filtered = true
			b, _ := json.Marshal(rulematch)
			dp.Attributes().PutStr(translate.CardinalFieldRuleConfig, string(b))
		}
		dp.Attributes().PutBool(translate.CardinalFieldFiltered, filtered)
		dp.Attributes().PutBool(translate.CardinalFieldWouldFilter, filtered)
		tid := translate.CalculateTID(rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes(), "metric")
		dp.Attributes().PutInt(translate.CardinalFieldTID, tid)
	}
	return aggregated
}

func (mp *metricProcessor) aggregateSum(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) int64 {
	aggregated := int64(0)
	for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
		dp := metric.Sum().DataPoints().At(i)
		filtered := false
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
		rulematch := mp.aggregateDatapoint(sampler.AggregationTypeSum, rms, ils, metric, dp, metadata)
		if rulematch != nil {
			aggregated++
			filtered = true
			b, _ := json.Marshal(rulematch)
			dp.Attributes().PutStr(translate.CardinalFieldRuleConfig, string(b))
		}
		dp.Attributes().PutBool(translate.CardinalFieldFiltered, filtered)
		dp.Attributes().PutBool(translate.CardinalFieldWouldFilter, filtered)
		tid := translate.CalculateTID(rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes(), "metric")
		dp.Attributes().PutInt(translate.CardinalFieldTID, tid)
	}
	return aggregated
}

// TODO Made public until we make it work to avoid linting errors
func (mp *metricProcessor) AggregateHistogram(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) int64 {
	aggregated := int64(0)
	metric.Histogram().DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
		attrs := dp.Attributes()
		buckets := dp.ExplicitBounds().AsRaw()
		ci := dp.BucketCounts().AsRaw()
		counts := make([]float64, len(ci))
		for i, c := range ci {
			counts[i] = float64(c)
		}
		t := dp.Timestamp().AsTime()
		// TODO pass in metadata
		rulematch, err := mp.aggregatorF.MatchAndAdd(&t, buckets, counts, sampler.AggregationTypeSum, metric.Name(), nil, rms.Resource().Attributes(), ils.Scope().Attributes(), attrs)
		if err != nil {
			mp.logger.Error("Error matching and adding histogram datapoint", zap.Error(err))
			return false
		}
		filtered := rulematch != nil
		if filtered {
			b, _ := json.Marshal(rulematch)
			dp.Attributes().PutStr(translate.CardinalFieldRuleConfig, string(b))
			aggregated++
		}
		dp.Attributes().PutBool(translate.CardinalFieldFiltered, filtered)
		dp.Attributes().PutBool(translate.CardinalFieldWouldFilter, filtered)
		tid := translate.CalculateTID(rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes(), "metric")
		dp.Attributes().PutInt(translate.CardinalFieldTID, tid)
		return false
	})
	return aggregated
}

func (mp *metricProcessor) aggregateDatapoint(
	ty sampler.AggregationType,
	rms pmetric.ResourceMetrics,
	ils pmetric.ScopeMetrics,
	metric pmetric.Metric,
	dp pmetric.NumberDataPoint,
	metadata map[string]string,
) *sampler.AggregatorConfig {
	t := dp.Timestamp().AsTime()
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v := dp.IntValue()
		rmatch, err := mp.aggregatorI.MatchAndAdd(
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
			mp.logger.Error("Error matching and adding int datapoint", zap.Error(err))
			return nil
		}
		return rmatch
	case pmetric.NumberDataPointValueTypeDouble:
		v := dp.DoubleValue()
		rmatch, err := mp.aggregatorF.MatchAndAdd(&t,
			[]float64{1},
			[]float64{v},
			ty,
			metric.Name(),
			metadata,
			rms.Resource().Attributes(),
			ils.Scope().Attributes(),
			dp.Attributes())
		if err != nil {
			mp.logger.Error("Error matching and adding float64 datapoint", zap.Error(err))
			return nil
		}
		return rmatch
	case pmetric.NumberDataPointValueTypeEmpty:
		return nil
	default:
		return nil
	}
}

func (mp *metricProcessor) Shutdown(_ context.Context) error {
	if mp.configManager != nil {
		mp.configManager.UnregisterCallback(mp.updaterId)
		mp.configManager.Stop()
	}
	return nil
}
