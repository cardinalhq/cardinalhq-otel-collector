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

package chqexemplarprocessor

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (p *exemplarProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if !p.config.Reporting.Metrics.Enabled {
		return md, nil
	}

	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		rattr := rm.Resource().Attributes()
		cid := orgIdFromResource(rattr)
		tenant := p.getTenant(cid)
		for j := range rm.ScopeMetrics().Len() {
			ilm := rm.ScopeMetrics().At(j)
			for k := range ilm.Metrics().Len() {
				m := ilm.Metrics().At(k)
				p.addMetricsExemplar(tenant, rm, ilm, m, m.Name(), m.Type())
			}
		}
	}

	return md, nil
}

func (p *exemplarProcessor) addMetricsExemplar(tenant *Tenant, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricName string, metricType pmetric.MetricType) {
	extraKeys := []string{
		metricNameKey, metricName,
		metricTypeKey, metricType.String(),
	}
	keys, exemplarKey := computeExemplarKey(rm.Resource(), extraKeys)
	if tenant.metricCache.Contains(exemplarKey) {
		return
	}
	exemplarRecord := toExemplar(rm, sm, mm, metricType)
	tenant.metricCache.Put(exemplarKey, keys, exemplarRecord)
}

func toExemplar(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricType pmetric.MetricType) pmetric.Metrics {
	exemplarRecord := pmetric.NewMetrics()
	copyRm := exemplarRecord.ResourceMetrics().AppendEmpty()
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
	return exemplarRecord
}
