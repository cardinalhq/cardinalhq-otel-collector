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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (c *chqDecorator) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	c.Lock()
	defer c.Unlock()

	environment := translate.EnvironmentFromEnv()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		c.metricsTransformations.ExecuteResourceMetricTransforms(rm)

		rattr := rm.Resource().Attributes()
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			c.metricsTransformations.ExecuteScopeMetricTransforms(sm, rm)

			sattr := sm.Scope().Attributes()
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				c.metricsTransformations.ExecuteMetricTransforms(m, sm, rm)

				extra := map[string]string{"name": m.Name()}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						c.metricsTransformations.ExecuteDataPointTransforms(dp, m, sm, rm)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						c.metricsTransformations.ExecuteDataPointTransforms(dp, m, sm, rm)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						c.metricsTransformations.ExecuteDataPointTransforms(dp, m, sm, rm)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						c.metricsTransformations.ExecuteDataPointTransforms(dp, m, sm, rm)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						c.metricsTransformations.ExecuteDataPointTransforms(dp, m, sm, rm)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				}
			}
		}
	}

	return md, nil
}

func (c *chqDecorator) updateMetricsTransformation(sc sampler.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating metrics transformations config", zap.String("vendor", c.vendor))
	// ok to ignore the parse error here, because we expect the config to be valid because it got validated
	// before it was saved by the UI.
	transformations, _ := ottl.ParseTransformations(sc.Metrics.Transformations, c.logger)
	c.metricsTransformations = transformations
}
