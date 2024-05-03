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

package table

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/trigram"
)

func (l *TableTranslator) MetricsFromOtel(om *pmetric.Metrics) ([]map[string]any, error) {
	rets := []map[string]any{}

	for i := 0; i < om.ResourceMetrics().Len(); i++ {
		rm := om.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			imm := rm.ScopeMetrics().At(j)
			for k := 0; k < imm.Metrics().Len(); k++ {
				baseret := map[string]any{"_cardinalhq.telemetry_type": "metric"}
				addAttributes(baseret, rm.Resource().Attributes(), "resource")
				addAttributes(baseret, imm.Scope().Attributes(), "scope")
				metric := imm.Metrics().At(k)
				rets = append(rets, l.toddmetric(metric, baseret)...)
			}
		}
	}

	return rets, nil
}

func (l *TableTranslator) toddmetric(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return l.toddGauge(metric, baseattrs)
	case pmetric.MetricTypeSum:
		return l.toddSum(metric, baseattrs)
	case pmetric.MetricTypeHistogram | pmetric.MetricTypeExponentialHistogram | pmetric.MetricTypeSummary:
		return nil
	default:
		return nil
	}
}

func (l *TableTranslator) toddGauge(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret["_cardinalhq.metric_type"] = "gauge"
		ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			val, safe := safeFloat(dp.DoubleValue())
			if !safe {
				continue
			}
			ret["_cardinalhq.value"] = val
		case pmetric.NumberDataPointValueTypeInt:
			ret["_cardinalhq.value"] = float64(dp.IntValue())
		default:
			continue
		}
		ret["_cardinalhq.name"] = metric.Name()
		ret["_cardinalhq.id"] = l.idg.Make(time.Now())
		ensureExpectedKeysMetrics(ret)
		rets = append(rets, ret)
	}

	return rets
}

func (l *TableTranslator) toddSum(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
		dp := metric.Sum().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret["_cardinalhq.metric_type"] = "gauge"
		ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			val, safe := safeFloat(dp.DoubleValue())
			if !safe {
				continue
			}
			ret["_cardinalhq.value"] = val
		case pmetric.NumberDataPointValueTypeInt:
			ret["_cardinalhq.value"] = float64(dp.IntValue())
		default:
			continue
		}
		ret["_cardinalhq.name"] = metric.Name()
		ret["_cardinalhq.id"] = l.idg.Make(time.Now())
		ensureExpectedKeysMetrics(ret)
		rets = append(rets, ret)
	}

	return rets
}

func safeFloat(v float64) (float64, bool) {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0, false
	}
	return v, true
}

func ensureExpectedKeysMetrics(m map[string]any) {
	keys := map[string]any{
		"_cardinalhq.rule_id":       "",
		"_cardinalhq.cluster_id":    "",
		"_cardinalhq.aggregated_by": "",
		"_cardinalhq.metric_type":   "gauge",
		"_cardinalhq.service":       "unknown_service",
		"_cardinalhq.version":       "",
		"_cardinalhq.hostname":      findHostname(m),
		"_cardinalhq.message":       "",
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}

	m["_cardinalhq.tid"] = calculateTID(m)
}

func calculateTID(tags map[string]any) int64 {
	keys := []string{}
	for k := range tags {
		if k[0] != '_' {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	items := []string{}
	for _, k := range keys {
		v := valueToString(tags[k])
		if v != "" {
			items = append(items, v)
		}
	}
	return trigram.JavaHashcode(strings.Join(items, ":"))
}

func valueToString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
