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

package chqstatsprocessor

import (
	"context"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (p *statsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if !p.config.Statistics.Metrics.StatisticsEnabled && !p.config.Statistics.Metrics.ExemplarsEnabled {
		return md, nil
	}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		rattr := rm.Resource().Attributes()
		cid := OrgIdFromResource(rattr)
		tenant := p.getTenant(cid)

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				metricName := m.Name()

				if p.config.Statistics.Metrics.ExemplarsEnabled {
					p.addMetricsExemplar(tenant, rm, ilm, m, metricName, m.Type())
				}
			}
		}
	}

	return md, nil
}

func (p *statsProcessor) addMetricsExemplar(tenant *Tenant, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricName string, metricType pmetric.MetricType) {
	if p.pbPhase == chqpb.Phase_PRE {
		extraKeys := []string{
			MetricNameKey, metricName,
			MetricTypeKey, metricType.String(),
		}
		keys, exemplarKey := computeExemplarKey(rm.Resource(), extraKeys)
		if tenant.metricExemplars.Contains(exemplarKey) {
			return
		}
		exemplarLm := toExemplar(rm, sm, mm, metricType)
		tenant.metricExemplars.Put(exemplarKey, keys, exemplarLm)
	}
}

func toExemplar(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricType pmetric.MetricType) pmetric.Metrics {
	exemplarLm := pmetric.NewMetrics()
	copyRm := exemplarLm.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(copyRm.Resource())
	copySm := copyRm.ScopeMetrics().AppendEmpty()
	sm.Scope().CopyTo(copySm.Scope())
	copyMm := copySm.Metrics().AppendEmpty()
	copyMm.SetName(mm.Name())
	copyMm.SetDescription(mm.Description())
	copyMm.SetUnit(mm.Unit())

	switch metricType {
	case pmetric.MetricTypeGauge:
		if mm.Gauge().DataPoints().Len() > 0 {
			newGauge := copyMm.SetEmptyGauge()
			dp := mm.Gauge().DataPoints().At(0)
			ccd := newGauge.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeSum:
		if mm.Sum().DataPoints().Len() > 0 {
			newSum := copyMm.SetEmptySum()
			dp := mm.Sum().DataPoints().At(0)
			ccd := newSum.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeHistogram:
		if mm.Histogram().DataPoints().Len() > 0 {
			newHistogram := copyMm.SetEmptyHistogram()
			dp := mm.Histogram().DataPoints().At(0)
			ccd := newHistogram.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeSummary:
		if mm.Summary().DataPoints().Len() > 0 {
			newSummary := copyMm.SetEmptySummary()
			dp := mm.Summary().DataPoints().At(0)
			ccd := newSummary.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeExponentialHistogram:
		if mm.ExponentialHistogram().DataPoints().Len() > 0 {
			newExponentialHistogram := copyMm.SetEmptyExponentialHistogram()
			dp := mm.ExponentialHistogram().DataPoints().At(0)
			ccd := newExponentialHistogram.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	}
	return exemplarLm
}
