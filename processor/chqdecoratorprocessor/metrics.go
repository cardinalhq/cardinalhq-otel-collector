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

type decoratorMetricProcessor struct {
	telemetry           *decoratorProcessorTelemetry
	logger              *zap.Logger
	aggregationInterval int64
	nextConsumer        consumer.Metrics
	configManager       sampler.ConfigManager
	updaterId           int
	aggregatorI         sampler.MetricAggregator[int64]
	aggregatorF         sampler.MetricAggregator[float64]
	lastEmitCheck       time.Time
}

func newDecoratorMetricProcessor(set processor.CreateSettings, conf *Config, nextConsumer consumer.Metrics) (*decoratorMetricProcessor, error) {
	var err error
	dmp := &decoratorMetricProcessor{
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
		dmp.configManager = confmgr

		dmp.updaterId = confmgr.RegisterCallback("metricsampler", func(config sampler.SamplerConfig) {
			dmp.aggregatorI.Configure(config.Metrics.Aggregators)
			dmp.aggregatorF.Configure(config.Metrics.Aggregators)
		})
	}

	dpt, err := newDecoratorProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	dmp.telemetry = dpt

	set.Logger.Info(
		"Decorator processor configured",
	)

	return dmp, nil
}

func (dmp *decoratorMetricProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	dropped := int64(0)
	md.ResourceMetrics().RemoveIf(func(rms pmetric.ResourceMetrics) bool {
		rms.ScopeMetrics().RemoveIf(func(ils pmetric.ScopeMetrics) bool {
			ils.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				filtered := dmp.aggregate(rms, ils, metric)
				if filtered {
					dropped++
				}
				return filtered
			})
			return rms.ScopeMetrics().Len() == 0
		})
		return md.ResourceMetrics().Len() == 0
	})

	dmp.telemetry.record(triggerMetricDataPointsAggregated, dropped)

	dmp.emit()

	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

func (dmp *decoratorMetricProcessor) emit() {
	now := time.Now()
	if now.Sub(dmp.lastEmitCheck) < time.Duration(dmp.aggregationInterval)*time.Second {
		return
	}
	dmp.lastEmitCheck = now
	mi := dmp.aggregatorI.Emit(now)
	for _, set := range mi {
		dmp.emitSetI(set)
	}
	mf := dmp.aggregatorF.Emit(now)
	for _, set := range mf {
		dmp.emitSetF(set)
	}
}

func (dmp *decoratorMetricProcessor) emitSetI(set *sampler.AggregationSet[int64]) {
	for _, agg := range set.Aggregations {
		dmp.logger.Info("Emitting int aggregated metric",
			zap.String("name", agg.Tags["_cardinalhq.name"]),
			zap.String("type", agg.AggregationType),
			zap.Any("tags", agg.Tags),
			zap.Int64("value", agg.Value()),
		)
	}
}

func (dmp *decoratorMetricProcessor) emitSetF(set *sampler.AggregationSet[float64]) {
	for _, agg := range set.Aggregations {
		dmp.logger.Info("Emitting float64 aggregated metric",
			zap.String("name", agg.Tags["_cardinalhq.name"]),
			zap.String("type", agg.AggregationType),
			zap.Any("tags", agg.Tags),
			zap.Float64("value", agg.Value()),
		)
	}
}

func (dmp *decoratorMetricProcessor) aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) bool {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return dmp.aggregateGauge(rms, ils, metric)
	case pmetric.MetricTypeSum:
		return dmp.aggregateSum(rms, ils, metric)
	case pmetric.MetricTypeHistogram:
		return false
	case pmetric.MetricTypeExponentialHistogram:
		return false
	case pmetric.MetricTypeSummary:
		return false
	default:
		return false
	}
}

func (dmp *decoratorMetricProcessor) aggregateGauge(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) bool {
	metric.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
		return dmp.aggregateDatapoint("avg", rms, ils, metric, dp)
	})
	return metric.Gauge().DataPoints().Len() == 0
}

func (dmp *decoratorMetricProcessor) aggregateSum(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) bool {
	metric.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
		return dmp.aggregateDatapoint("sum", rms, ils, metric, dp)
	})
	return metric.Sum().DataPoints().Len() == 0
}

func (dmp *decoratorMetricProcessor) aggregateDatapoint(ty string, rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric, dp pmetric.NumberDataPoint) bool {
	t := dp.Timestamp().AsTime()
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v := dp.IntValue()
		rmatch := dmp.aggregatorI.MatchAndAdd(&t, v, ty, metric.Name(), rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes())
		return rmatch != ""
	case pmetric.NumberDataPointValueTypeDouble:
		v := dp.DoubleValue()
		rmatch := dmp.aggregatorF.MatchAndAdd(&t, v, ty, metric.Name(), rms.Resource().Attributes(), ils.Scope().Attributes(), dp.Attributes())
		return rmatch != ""
	default:
		return false
	}
}
