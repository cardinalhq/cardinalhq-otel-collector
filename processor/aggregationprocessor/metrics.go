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
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/telemetry"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *pitbull) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}

	environment := translate.EnvironmentFromEnv()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rattr := rm.Resource().Attributes()
		serviceName := ottl.GetServiceName(rm.Resource())
		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			sattr := ilm.Scope().Attributes()
			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				metricName := m.Name()
				extra := map[string]string{"name": m.Name()}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						agg := e.aggregate(rm, ilm, m, dp)
						if agg {
							telemetry.CounterAdd(e.aggregatedDatapoints, 1, metric.WithAttributes(
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
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						agg := e.aggregate(rm, ilm, m, dp)
						if agg {
							telemetry.CounterAdd(e.aggregatedDatapoints, 1, metric.WithAttributes(
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
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						return false
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						return false
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						dattr := dp.Attributes()
						if !needsAttr(dattr) {
							return false
						}
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						return false
					})
				}

				return false
			})
			return ilm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	e.emit(time.Now())

	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

func needsAttr(attrs pcommon.Map) bool {
	_, ok := attrs.Get(translate.CardinalFieldAggregate)
	return ok
}
