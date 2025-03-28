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

package aggregationprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (p *aggregationProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		serviceName := ottl.GetServiceName(rm.Resource())
		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				metricName := m.Name()
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						dattr := dp.Attributes()

						if !needsAttr(dattr) {
							return false
						}
						agg := p.aggregate(rm, ilm, m, dp)
						if agg {
							telemetry.CounterAdd(p.aggregatedDatapoints, 1, metric.WithAttributes(
								attribute.String("type", "gauge"),
								attribute.String("metric_name", metricName),
								attribute.String("service_name", serviceName)))
							return agg
						}
						return false
					})
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						agg := p.aggregate(rm, ilm, m, dp)
						if agg {
							telemetry.CounterAdd(p.aggregatedDatapoints, 1, metric.WithAttributes(
								attribute.String("type", "sum"),
								attribute.String("metric_name", metricName),
								attribute.String("service_name", serviceName)))
							return agg
						}
						return false
					})
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						return false
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						return false
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						return false
					})
				case pmetric.MetricTypeEmpty:
					return false
				default:
					return false
				}

				return false
			})
			return ilm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	p.emit(time.Now())

	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

func needsAttr(attrs pcommon.Map) bool {
	_, ok := attrs.Get(translate.CardinalFieldAggregate)
	return ok
}
