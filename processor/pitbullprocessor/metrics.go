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

package pitbullprocessor

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *pitbull) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}

	emptySlice := pcommon.NewSlice()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rattr := rm.Resource().Attributes()
		transformCtx := ottlresource.NewTransformContext(rm.Resource(), rm)
		e.metricTransformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
		if _, found := rattr.Get(translate.CardinalFieldDropMarker); found {
			return true
		}

		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			sattr := ilm.Scope().Attributes()
			transformCtx := ottlscope.NewTransformContext(ilm.Scope(), rm.Resource(), rm)
			e.metricTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
			if _, found := sattr.Get(translate.CardinalFieldDropMarker); found {
				return true
			}

			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				}

				return false
			})
			return ilm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

func (c *pitbull) updateMetricTransformation(sc ottl.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating metrics transformations", zap.Int("num_decorators", len(sc.Metrics.Decorators)))
	c.logger.Info("Metrics decorators", zap.Any("decorators", sc.Metrics.Decorators))
	newTransformations := ottl.NewTransformations(c.logger)

	for _, decorator := range sc.Metrics.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing metrics transformation", zap.Error(err))
		} else {
			newTransformations = ottl.MergeWith(newTransformations, transformations)
		}
	}

	oldTransformation := c.metricTransformations
	c.metricTransformations = newTransformations
	oldTransformation.Stop()
}
