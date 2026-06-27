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
	"math"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (p *summarysplit) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if countMetricTypes(md)[pmetric.MetricTypeSummary] == 0 {
		return md, nil
	}
	result, split := splitSummaryDataPoints(md)
	if p.splitCount != nil {
		p.splitCount.Add(ctx, split)
	}
	return result, nil
}

func countMetricTypes(md pmetric.Metrics) map[pmetric.MetricType]int64 {
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

// splitSummaryDataPoints rebuilds md, replacing each summary metric with its
// count/sum/quantile metrics and passing everything else through unchanged. It
// returns the number of summary data points that were split.
func splitSummaryDataPoints(md pmetric.Metrics) (pmetric.Metrics, int64) {
	var split int64
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
					split += int64(metric.Summary().DataPoints().Len())
					splitSummaryDataPoint(metric, newILM)
				} else {
					metric.CopyTo(newILM.Metrics().AppendEmpty())
				}
			}
		}
	}
	// Drop scopes/resources we left empty (e.g. a scope whose only metrics were
	// summaries with no data points) rather than emitting empty trees.
	newMetrics.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	return newMetrics, split
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
	// .count is emitted as a delta (non-monotonic) Sum per Cardinal's convention.
	count.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	for i := 0; i < summary.DataPoints().Len(); i++ {
		sdp := summary.DataPoints().At(i)
		dp := count.DataPoints().AppendEmpty()
		sdp.Attributes().CopyTo(dp.Attributes())
		dp.SetStartTimestamp(startTimestamp(sdp))
		dp.SetTimestamp(sdp.Timestamp())
		// Count is uint64 but the data point only holds int64; clamp rather than wrap.
		count := sdp.Count()
		if count > math.MaxInt64 {
			count = math.MaxInt64
		}
		dp.SetIntValue(int64(count))
	}
}

func createSumMetric(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	summary := metric.Summary()
	msum := ilm.Metrics().AppendEmpty()
	msum.SetDescription(metric.Description())
	msum.SetUnit(metric.Unit())
	msum.SetName(metric.Name() + ".sum")
	// Summary.sum isn't guaranteed monotonic across reports, so emit it as a gauge.
	g := msum.SetEmptyGauge()
	for i := 0; i < summary.DataPoints().Len(); i++ {
		sdp := summary.DataPoints().At(i)
		dp := g.DataPoints().AppendEmpty()
		sdp.Attributes().CopyTo(dp.Attributes())
		dp.SetStartTimestamp(startTimestamp(sdp))
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
			q := quantile.Quantile()
			// Skip out-of-domain quantiles so we don't mint junk metric names.
			if math.IsNaN(q) || math.IsInf(q, 0) || q < 0 || q > 1 {
				continue
			}
			name := quantileToName(metric.Name(), q)
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
			dp.SetStartTimestamp(startTimestamp(sdp))
			dp.SetTimestamp(sdp.Timestamp())
			dp.SetDoubleValue(quantile.Value())
		}
	}
}

// startTimestamp returns the data point's StartTimestamp, falling back to its
// Timestamp when unset. Some receivers (e.g. awsfirehose) omit StartTimestamp,
// and a zero start time breaks delta-temporality consumers downstream.
func startTimestamp(sdp pmetric.SummaryDataPoint) pcommon.Timestamp {
	if sts := sdp.StartTimestamp(); sts != 0 {
		return sts
	}
	return sdp.Timestamp()
}

// quantileToName maps a quantile to a metric-name suffix: 0 -> .min, 1 -> .max,
// otherwise .quantile.<percent> with '.' replaced by '_' (e.g. 0.999 -> .quantile.99_9).
func quantileToName(baseName string, quantile float64) string {
	switch quantile {
	case 0:
		return baseName + ".min"
	case 1:
		return baseName + ".max"
	default:
		quantileStr := strconv.FormatFloat(quantile*100, 'f', -1, 64)
		return baseName + ".quantile." + strings.ReplaceAll(quantileStr, ".", "_")
	}
}
