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

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type decoratorMetricProcessor struct {
	telemetry *decoratorProcessorTelemetry
	logger    *zap.Logger
}

func newDecoratorMetricProcessor(set processor.CreateSettings, _ *Config) (*decoratorMetricProcessor, error) {
	var err error
	dsp := &decoratorMetricProcessor{
		logger: set.Logger,
	}

	dpt, err := newDecoratorProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	dsp.telemetry = dpt

	set.Logger.Info(
		"Decorator processor configured",
	)

	return dsp, nil
}

func (dmp *decoratorMetricProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		//resource := rm.Resource()
		ilms := rm.ScopeMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ils := ilms.At(j)
			//scope := ils.Scope()
			metrics := ils.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				dmp.augment(metric)
			}
		}
	}

	return md, nil
}

func (dmp *decoratorMetricProcessor) augment(metric pmetric.Metric) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dp.Attributes().PutStr("cardinalhq.was", "here.gauge")
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dp.Attributes().PutStr("cardinalhq.was", "here.sum")
		}
	case pmetric.MetricTypeHistogram:
		dps := metric.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dp.Attributes().PutStr("cardinalhq.was", "here.histogram")
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dp.Attributes().PutStr("cardinalhq.was", "here.exponential_histogram")
		}
	case pmetric.MetricTypeSummary:
		dps := metric.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			dp.Attributes().PutStr("cardinalhq.was", "here.summary")
		}
	default:
	}
}
