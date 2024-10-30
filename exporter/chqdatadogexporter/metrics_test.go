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

package chqdatadogexporter

import (
	"context"
	"testing"
	"time"

	ddpb "github.com/cardinalhq/cardinalhq-otel-collector/internal/ddpb"
	"github.com/tj/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestValueAsFloat64(t *testing.T) {
	pi := pmetric.NewNumberDataPoint()
	pi.SetDoubleValue(3.14)
	assert.Equal(t, 3.14, valueAsFloat64(pi))

	meaningoflife := pmetric.NewNumberDataPoint()
	meaningoflife.SetIntValue(42)
	assert.Equal(t, 42.0, valueAsFloat64(meaningoflife))
}

func TestConvertSumMetric(t *testing.T) {
	ctx := context.Background()
	exporter := &datadogExporter{}

	metric := pmetric.NewMetric()
	metric.SetName("test.sum.metric")
	metric.SetUnit("ms")

	rAttr := pcommon.NewMap()
	rAttr.PutStr("resource_key", "resource_value")

	sAttr := pcommon.NewMap()
	sAttr.PutStr("scope_key", "scope_value")

	sum := pmetric.NewSum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(100)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.Attributes().PutStr("dp_key", "dp_value")

	series := exporter.convertSumMetric(ctx, metric, rAttr, sAttr, sum)

	assert.Len(t, series, 1)
	assert.Equal(t, "test.sum.metric", series[0].Metric)
	assert.Equal(t, "ms", series[0].Unit)
	assert.Equal(t, ddpb.MetricPayload_COUNT, series[0].Type)
	assert.ElementsMatch(t,
		[]string{
			"resource_key:resource_value",
			"scope_key:scope_value",
			"dp_key:dp_value",
		}, series[0].Tags)
	assert.Len(t, series[0].Points, 1)
	assert.Equal(t, float64(100), series[0].Points[0].Value)
}

func TestConvertSumMetricWithInterval(t *testing.T) {
	ctx := context.Background()
	exporter := &datadogExporter{}

	metric := pmetric.NewMetric()
	metric.SetName("test.sum.metric.interval")
	metric.SetUnit("ms")

	rAttr := pcommon.NewMap()
	rAttr.PutStr("resource_key", "resource_value")

	sAttr := pcommon.NewMap()
	sAttr.PutStr("scope_key", "scope_value")

	sum := pmetric.NewSum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(false)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(100)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.Attributes().PutStr("dp_key", "dp_value")
	dp.Attributes().PutInt("_dd.rateInterval", 10)

	series := exporter.convertSumMetric(ctx, metric, rAttr, sAttr, sum)

	assert.Len(t, series, 1)
	assert.Equal(t, "test.sum.metric.interval", series[0].Metric)
	assert.Equal(t, "ms", series[0].Unit)
	assert.Equal(t, ddpb.MetricPayload_RATE, series[0].Type)
	assert.Equal(t, int64(10), series[0].Interval)
	assert.ElementsMatch(t,
		[]string{
			"resource_key:resource_value",
			"scope_key:scope_value",
			"dp_key:dp_value",
		}, series[0].Tags)
	assert.Len(t, series[0].Points, 1)
	assert.Equal(t, float64(10), series[0].Points[0].Value) // 100 / 10
}

func TestGetInterval(t *testing.T) {
	tests := []struct {
		name     string
		attrs    pcommon.Map
		expected int64
		found    bool
	}{
		{
			name:     "NoInterval",
			attrs:    pcommon.NewMap(),
			expected: 1,
			found:    false,
		},
		{
			name: "ValidInt64Interval",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutInt("_dd.rateInterval", 10)
				return m
			}(),
			expected: 10,
			found:    true,
		},
		{
			name: "InvalidInt64Interval",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutInt("_dd.rateInterval", -5)
				return m
			}(),
			expected: 1,
			found:    true,
		},
		{
			name: "ValidStringInterval",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("_dd.rateInterval", "15")
				return m
			}(),
			expected: 15,
			found:    true,
		},
		{
			name: "InvalidStringInterval",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("_dd.rateInterval", "-20")
				return m
			}(),
			expected: 1,
			found:    true,
		},
		{
			name: "NonNumericStringInterval",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("_dd.rateInterval", "invalid")
				return m
			}(),
			expected: 1,
			found:    false,
		},
		{
			name: "UnsupportedTypeInterval",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutDouble("_dd.rateInterval", 5.5)
				return m
			}(),
			expected: 1,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interval, found := getInterval(tt.attrs)
			assert.Equal(t, tt.expected, interval)
			assert.Equal(t, tt.found, found)
		})
	}
}
