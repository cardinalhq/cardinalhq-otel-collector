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
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
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
	case pmetric.MetricTypeHistogram:
		return l.toddHistogram(metric, baseattrs)
	case pmetric.MetricTypeExponentialHistogram:
		return l.toddExponentialHistogram(metric, baseattrs)
	case pmetric.MetricTypeSummary:
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

func (l *TableTranslator) toddHistogram(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	metricType := "histogram"

	for i := 0; i < metric.Histogram().DataPoints().Len(); i++ {
		dp := metric.Histogram().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret["_cardinalhq.metric_type"] = metricType
		ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
		ret["_cardinalhq.counts"] = asJson(dp.BucketCounts().AsRaw())
		ret["_cardinalhq.bucket_bounds"] = asJson(dp.ExplicitBounds().AsRaw())
		ret["_cardinalhq.name"] = metric.Name()
		ret["_cardinalhq.id"] = l.idg.Make(time.Now())
		ret["_cardinalhq.value"] = float64(-1)
		ensureExpectedKeysMetrics(ret)
		rets = append(rets, ret)
	}

	return rets
}

func (l *TableTranslator) toddExponentialHistogram(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	metricType := "exponential_histogram"

	for i := 0; i < metric.ExponentialHistogram().DataPoints().Len(); i++ {
		dp := metric.ExponentialHistogram().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret["_cardinalhq.metric_type"] = metricType
		ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
		ret["_cardinalhq.scale"] = dp.Scale()
		ret["_cardinalhq.negative.counts"] = asJson(dp.Negative().BucketCounts().AsRaw())
		ret["_cardinalhq.positive.counts"] = asJson(dp.Positive().BucketCounts().AsRaw())
		ret["_cardinalhq.zero.count"] = dp.ZeroCount()
		ret["_cardinalhq.name"] = metric.Name()
		ret["_cardinalhq.id"] = l.idg.Make(time.Now())
		ret["_cardinalhq.value"] = float64(-1)
		ensureExpectedKeysMetrics(ret)
		rets = append(rets, ret)
	}

	return rets
}

func asJson[T uint64 | float64](s []T) string {
	ret, _ := json.Marshal(s)
	return string(ret)
}

func ensureExpectedKeysMetrics(m map[string]any) {
	keys := map[string]any{
		"_cardinalhq.ruleconfig":  "",
		"_cardinalhq.metric_type": "gauge",
		"_cardinalhq.hostname":    findHostname(m),
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

type DDWrapper struct {
	Sketch         *ddsketch.DDSketch
	StartTimestamp time.Time
	Timestamp      time.Time
	Attributes     map[string]any
}

// TODO this is likely going to be useful someday, but not today...
// nolint:unused
func convertToDDSketch(dp pmetric.ExponentialHistogramDataPoint) (*DDWrapper, error) {
	// Create a new DDSketch with a relative accuracy of 0.01
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		return nil, err
	}

	// Add the counts from each bucket to the sketch
	for bucketIndex, count := range dp.Positive().BucketCounts().AsRaw() {
		// Calculate the bucket value based on the scale and index
		bucketValue := math.Pow(2, float64(dp.Scale())) * float64(bucketIndex)
		err := sketch.AddWithCount(bucketValue, float64(count))
		if err != nil {
			return nil, err
		}
	}

	for bucketIndex, count := range dp.Negative().BucketCounts().AsRaw() {
		// Calculate the bucket value based on the scale and index
		bucketValue := -math.Pow(2, float64(dp.Scale())) * float64(bucketIndex)
		if err := sketch.AddWithCount(bucketValue, float64(count)); err != nil {
			return nil, err
		}
	}

	// Add the zero count to the sketch
	if err := sketch.AddWithCount(0, float64(dp.ZeroCount())); err != nil {
		return nil, err
	}

	dw := &DDWrapper{
		Sketch:         sketch,
		StartTimestamp: dp.StartTimestamp().AsTime(),
		Timestamp:      dp.Timestamp().AsTime(),
		Attributes:     dp.Attributes().AsRaw(),
	}
	return dw, nil
}
