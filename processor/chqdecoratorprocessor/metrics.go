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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func (c *chqDecorator) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	c.Lock()
	defer c.Unlock()

	environment := translate.EnvironmentFromEnv()
	transformations := c.metricsTransformations
	emptySlice := pcommon.NewSlice()

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceCtx := ottlresource.NewTransformContext(rm.Resource(), rm)
		transformations.ExecuteResourceTransforms(c.ottlProcessed, resourceCtx, "", emptySlice)

		rattr := rm.Resource().Attributes()
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scopeCtx := ottlscope.NewTransformContext(sm.Scope(), rm.Resource(), sm)
			transformations.ExecuteScopeTransforms(c.ottlProcessed, scopeCtx, "", emptySlice)

			sattr := sm.Scope().Attributes()
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				metricsCtx := ottlmetric.NewTransformContext(m, sm.Metrics(), sm.Scope(), rm.Resource(), sm, rm)
				transformations.ExecuteMetricTransforms(c.ottlProcessed, metricsCtx, "", emptySlice)

				extra := map[string]string{"name": m.Name()}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						dataPointCtx := ottldatapoint.NewTransformContext(dp, m, sm.Metrics(), sm.Scope(), rm.Resource(), sm, rm)
						transformations.ExecuteDataPointTransforms(c.ottlProcessed, dataPointCtx, "", emptySlice)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						dataPointCtx := ottldatapoint.NewTransformContext(dp, m, sm.Metrics(), sm.Scope(), rm.Resource(), sm, rm)
						transformations.ExecuteDataPointTransforms(c.ottlProcessed, dataPointCtx, "", emptySlice)

						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						dataPointCtx := ottldatapoint.NewTransformContext(dp, m, sm.Metrics(), sm.Scope(), rm.Resource(), sm, rm)
						transformations.ExecuteDataPointTransforms(c.ottlProcessed, dataPointCtx, "", emptySlice)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						dataPointCtx := ottldatapoint.NewTransformContext(dp, m, sm.Metrics(), sm.Scope(), rm.Resource(), sm, rm)
						transformations.ExecuteDataPointTransforms(c.ottlProcessed, dataPointCtx, "", emptySlice)
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						dataPointCtx := ottldatapoint.NewTransformContext(dp, m, sm.Metrics(), sm.Scope(), rm.Resource(), sm, rm)
						transformations.ExecuteDataPointTransforms(c.ottlProcessed, dataPointCtx, "", emptySlice)
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

func (c *chqDecorator) updateMetricsTransformation(sc ottl.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating metrics transformations", zap.Int("num_decorators", len(sc.Metrics.Decorators)))
	newTransformations := ottl.NewTransformations(c.logger)

	for _, decorator := range sc.Metrics.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing log transformation", zap.Error(err))
		} else {
			newTransformations = ottl.MergeWith(newTransformations, transformations)
		}
	}

	oldTransformation := c.metricsTransformations
	c.metricsTransformations = newTransformations
	oldTransformation.Stop()
}
