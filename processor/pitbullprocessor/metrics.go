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

package pitbullprocessor

import (
	"context"
	"log/slog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (p *pitbull) ConsumeMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.DataPointCount() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rattr := rm.Resource().Attributes()
		cid := orgIDFromResource(rattr)
		transformations, transformationsFound := p.metricTransformations.Load(cid)
		luc, lucFound := p.metricsLookupConfigs.Load(cid)
		if !transformationsFound && !lucFound {
			return false
		}
		attrSet := p.attributesFor(cid)

		if transformations != nil {
			transformCtx := ottlresource.NewTransformContext(rm.Resource(), rm)
			transformations.ExecuteResourceTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
			if _, found := rattr.Get(translate.CardinalFieldDropMarker); found {
				return true
			}
		}

		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			if transformations != nil {
				sattr := ilm.Scope().Attributes()
				transformCtx := ottlscope.NewTransformContext(ilm.Scope(), rm.Resource(), rm)
				transformations.ExecuteScopeTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
				if _, found := sattr.Get(translate.CardinalFieldDropMarker); found {
					return true
				}
			}

			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						p.evaluateLookupTables(transformCtx, luc, func(tagToSet string, targetValue string) {
							dp.Attributes().PutStr(tagToSet, targetValue)
						})
						if transformations == nil {
							return false
						}
						transformations.ExecuteDatapointTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						p.evaluateLookupTables(transformCtx, luc, func(tagToSet string, targetValue string) {
							dp.Attributes().PutStr(tagToSet, targetValue)
						})
						if transformations == nil {
							return false
						}
						transformations.ExecuteDatapointTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						p.evaluateLookupTables(transformCtx, luc, func(tagToSet string, targetValue string) {
							dp.Attributes().PutStr(tagToSet, targetValue)
						})
						if transformations == nil {
							return false
						}
						transformations.ExecuteDatapointTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						p.evaluateLookupTables(transformCtx, luc, func(tagToSet string, targetValue string) {
							dp.Attributes().PutStr(tagToSet, targetValue)
						})
						if transformations == nil {
							return false
						}
						transformations.ExecuteDatapointTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
						_, found := dp.Attributes().Get(translate.CardinalFieldDropMarker)
						return found
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						p.evaluateLookupTables(transformCtx, luc, func(tagToSet string, targetValue string) {
							dp.Attributes().PutStr(tagToSet, targetValue)
						})
						if transformations == nil {
							return false
						}
						transformations.ExecuteDatapointTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
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

	if md.DataPointCount() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}

	slog.Info("Metrics transformed", slog.Int("resourceMetrics", md.ResourceMetrics().Len()))
	return md, nil
}

func (p *pitbull) evaluateLookupTables(transformCtx ottldatapoint.TransformContext, configs *[]ottl.LookupConfig, handlerFunc func(tagToSet string, targetValue string)) {
	if configs == nil {
		return
	}
	for _, lookupConfig := range *configs {
		lookupConfig.ExecuteMetricsRules(context.Background(), transformCtx, handlerFunc)
	}
}

func (p *pitbull) updateMetricConfigForTenant(cid string, pbc *ottl.PitbullProcessorConfig) {
	if pbc == nil {
		p.shutdownMetricsForTenant(cid)
		return
	}
	p.logger.Info("Updating metrics transformations", zap.String("organizationID", cid), zap.Int("metricTransformations", len(pbc.MetricStatements)), zap.Int("metricLookupConfigs", len(pbc.MetricLookupConfigs)))

	newTransformations := ottl.NewTransformations()
	transformations, err := ottl.ParseTransformations(p.logger, pbc.MetricStatements)
	if err != nil {
		p.logger.Error("Error parsing metrics transformation", zap.Error(err))
	} else {
		newTransformations = ottl.MergeWith(newTransformations, transformations)
	}

	if old, found := p.metricTransformations.Replace(cid, newTransformations); found {
		old.Stop()
	}

	for _, lookupConfig := range pbc.MetricLookupConfigs {
		lookupConfig.Init(p.logger)
	}
	p.metricsLookupConfigs.Store(cid, &pbc.MetricLookupConfigs)
}
