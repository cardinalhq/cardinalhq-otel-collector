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

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

type metricProcessor struct {
	logger *zap.Logger
}

func newMetricProcessor(set processor.Settings) (*metricProcessor, error) {
	return &metricProcessor{
		logger: set.Logger,
	}, nil
}

func (mp *metricProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		rattr := rm.Resource().Attributes()
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			sattr := sm.Scope().Attributes()
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(rattr, sattr, dattr, "metric")
						dattr.PutInt(translate.CardinalFieldTID, tid)
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(rattr, sattr, dattr, "metric")
						dattr.PutInt(translate.CardinalFieldTID, tid)
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(rattr, sattr, dattr, "metric")
						dattr.PutInt(translate.CardinalFieldTID, tid)
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(rattr, sattr, dattr, "metric")
						dattr.PutInt(translate.CardinalFieldTID, tid)
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(rattr, sattr, dattr, "metric")
						dattr.PutInt(translate.CardinalFieldTID, tid)
					}
				}
			}
		}
	}

	return md, nil
}

func (mp *metricProcessor) Shutdown(_ context.Context) error {
	return nil
}
