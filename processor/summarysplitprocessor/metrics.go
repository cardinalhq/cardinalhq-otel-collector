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

package summarysplitprocessor

import (
	"context"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (p *summarysplit) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	inCount := countDatapointTypes(md)
	if inCount[pmetric.MetricTypeSummary] == 0 {
		return md, nil
	}
	result := splitSummaryDataPoints(md)
	return result, nil
}

func countDatapointTypes(md pmetric.Metrics) map[pmetric.MetricType]int64 {
	counts := map[pmetric.MetricType]int64{}
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				metric := ilm.Metrics().At(k)
				counts[metric.Type()]++
			}
		}
	}
	return counts
}

func splitSummaryDataPoints(md pmetric.Metrics) pmetric.Metrics {
	newMetrics := pmetric.NewMetrics()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		newRM := newMetrics.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(newRM.Resource())
		newRM.SetSchemaUrl(rm.SchemaUrl())
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			newILM := newRM.ScopeMetrics().AppendEmpty()
			ilm.Scope().CopyTo(newILM.Scope())
			newILM.SetSchemaUrl(ilm.SchemaUrl())
			for k := 0; k < ilm.Metrics().Len(); k++ {
				metric := ilm.Metrics().At(k)
				if metric.Type() == pmetric.MetricTypeSummary {
					splitSummaryDataPoint(metric, newILM)
				} else {
					metric.CopyTo(newILM.Metrics().AppendEmpty())
				}
			}
		}
	}
	return newMetrics
}

func splitSummaryDataPoint(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	summary := metric.Summary()
	if summary.DataPoints().Len() == 0 {
		return
	}

	createCountMetric(metric, ilm)
	createSumMetric(metric, ilm)
	createQuantileMetrics(metric, ilm)
}

func createCountMetric(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	summary := metric.Summary()
	mcount := ilm.Metrics().AppendEmpty()
	mcount.SetDescription(metric.Description())
	mcount.SetUnit(metric.Unit())
	mcount.SetName(metric.Name() + ".count")
	count := mcount.SetEmptySum()
	count.SetIsMonotonic(false)
	count.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	for i := 0; i < summary.DataPoints().Len(); i++ {
		sdp := summary.DataPoints().At(i)
		dp := count.DataPoints().AppendEmpty()
		sdp.Attributes().CopyTo(dp.Attributes())
		sts := sdp.StartTimestamp()
		if sts == 0 {
			sts = sdp.Timestamp()
		}
		dp.SetStartTimestamp(sts)
		dp.SetTimestamp(sdp.Timestamp())
		dp.SetIntValue(int64(sdp.Count()))
	}
}

func createSumMetric(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	summary := metric.Summary()
	msum := ilm.Metrics().AppendEmpty()
	msum.SetDescription(metric.Description())
	msum.SetUnit(metric.Unit())
	msum.SetName(metric.Name() + ".sum")
	g := msum.SetEmptyGauge()
	for i := 0; i < summary.DataPoints().Len(); i++ {
		sdp := summary.DataPoints().At(i)
		dp := g.DataPoints().AppendEmpty()
		sdp.Attributes().CopyTo(dp.Attributes())
		sts := sdp.StartTimestamp()
		if sts == 0 {
			sts = sdp.Timestamp()
		}
		dp.SetStartTimestamp(sts)
		dp.SetTimestamp(sdp.Timestamp())
		dp.SetDoubleValue(sdp.Sum())
	}
}

func createQuantileMetrics(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	summary := metric.Summary()
	metricRefs := map[string]pmetric.Metric{}
	for i := 0; i < summary.DataPoints().Len(); i++ {
		sdp := summary.DataPoints().At(i)
		for j := 0; j < sdp.QuantileValues().Len(); j++ {
			quantile := sdp.QuantileValues().At(j)
			name := quantileToName(metric.Name(), quantile.Quantile())
			m, ok := metricRefs[name]
			if !ok {
				m = ilm.Metrics().AppendEmpty()
				m.SetName(name)
				m.SetEmptyGauge()
				m.SetDescription(metric.Description())
				m.SetUnit(metric.Unit())
				metricRefs[name] = m
			}
			dp := m.Gauge().DataPoints().AppendEmpty()
			sdp.Attributes().CopyTo(dp.Attributes())
			sts := sdp.StartTimestamp()
			if sts == 0 {
				sts = sdp.Timestamp()
			}
			dp.SetStartTimestamp(sts)
			dp.SetTimestamp(sdp.Timestamp())
			dp.SetDoubleValue(quantile.Value())
		}
	}
}

func quantileToName(baseName string, quantile float64) string {
	switch quantile {
	case 0:
		return baseName + ".min"
	case 1:
		return baseName + ".max"
	default:
		return quantileToNameSuffix(baseName, quantile)
	}
}

func quantileToNameSuffix(baseName string, quantile float64) string {
	quantileStr := strconv.FormatFloat(quantile*100, 'f', -1, 64)
	return baseName + ".quantile." + strings.ReplaceAll(quantileStr, ".", "_")
}
