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
	"fmt"
	"time"

	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/sampler"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
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
	md.ResourceMetrics().RemoveIf(func(rms pmetric.ResourceMetrics) bool {
		rms.ScopeMetrics().RemoveIf(func(ils pmetric.ScopeMetrics) bool {
			ils.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				aggregated = aggregated + mp.aggregate(rms, ils, metric)
				// decorate, don't drop
				return false
			})
			return rms.ScopeMetrics().Len() == 0
		})
		return md.ResourceMetrics().Len() == 0
	})

	mp.telemetry.record(triggerMetricDataPointsAggregated, aggregated)

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
	for tsb, agg := range set.Aggregations {
		mp.logger.Info("Emitting int aggregated metric",
			zap.Uint64("tsb", tsb),
			zap.String("name", agg.Name()),
			zap.String("type", agg.AggregationType().String()),
			zap.Any("tags", agg.Tags()),
			zap.Int64s("buckets", agg.Buckets()),
			zap.Int64s("values", agg.Value()),
		)
	}
}

func (mp *metricProcessor) emitSetF(set *sampler.AggregationSet[float64]) {
	for tsb, agg := range set.Aggregations {
		mp.logger.Info("Emitting float64 aggregated metric",
			zap.Uint64("tsb", tsb),
			zap.String("name", agg.Name()),
			zap.String("type", agg.AggregationType().String()),
			zap.Any("tags", agg.Tags()),
			zap.Float64s("buckets", agg.Buckets()),
			zap.Float64s("values", agg.Value()),
		)
	}
}

func (mp *metricProcessor) aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) int64 {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return mp.aggregateGauge(rms, ils, metric)
	case pmetric.MetricTypeSum:
		return mp.aggregateSum(rms, ils, metric)
	case pmetric.MetricTypeHistogram:
		return mp.AggregateHistogram(rms, ils, metric)
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
		dp.Attributes().PutStr("_cardinalhq.was", "here")
		filtered := false
		if mp.aggregateDatapoint(sampler.AggregationTypeAvg, rms, ils, metric, dp) {
			aggregated++
			filtered = true
		}
		dp.Attributes().PutBool("_cardinalhq.filtered", filtered)
	}
	return aggregated
}

func (mp *metricProcessor) aggregateSum(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) int64 {
	aggregated := int64(0)
	for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
		dp := metric.Sum().DataPoints().At(i)
		dp.Attributes().PutStr("_cardinalhq.was", "here")
		filtered := false
		if mp.aggregateDatapoint(sampler.AggregationTypeSum, rms, ils, metric, dp) {
			aggregated++
			filtered = true
		}
		dp.Attributes().PutBool("_cardinalhq.filtered", filtered)
	}
	return aggregated
}

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
		rmatch, err := mp.aggregatorF.MatchAndAdd(&t, buckets, counts, sampler.AggregationTypeSum, metric.Name(), rms.Resource().Attributes(), ils.Scope().Attributes(), attrs)
		if err != nil {
			mp.logger.Error("Error matching and adding histogram datapoint", zap.Error(err))
			return false
		}
		filtered := rmatch != ""
		if filtered {
			dp.Attributes().PutBool("_cardinalhq.filtered", true)
			aggregated++
		}
		return false
	})
	return aggregated
}

func (mp *metricProcessor) aggregateDatapoint(ty sampler.AggregationType, rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	t := dp.Timestamp().AsTime()
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v := dp.IntValue()
		rmatch, err := mp.aggregatorI.MatchAndAdd(&t, []int64{1}, []int64{v}, ty, metric.Name(), rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes())
		if err != nil {
			mp.logger.Error("Error matching and adding int datapoint", zap.Error(err))
			return false
		}
		return rmatch != ""
	case pmetric.NumberDataPointValueTypeDouble:
		v := dp.DoubleValue()
		rmatch, err := mp.aggregatorF.MatchAndAdd(&t, []float64{1}, []float64{v}, ty, metric.Name(), rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes())
		if err != nil {
			mp.logger.Error("Error matching and adding float64 datapoint", zap.Error(err))
			return false
		}
		return rmatch != ""
	case pmetric.NumberDataPointValueTypeEmpty:
		return false
	default:
		return false
	}
}

func (mp *metricProcessor) Shutdown(_ context.Context) error {
	if mp.configManager != nil {
		mp.configManager.UnregisterCallback(mp.updaterId)
		mp.configManager.Stop()
	}
	return nil
}
