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

package summarysplitprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

func TestQuantileToName(t *testing.T) {
	tests := []struct {
		quantile float64
		basename string
		expected string
	}{
		{0.5, "test", "test.quantile.50"},
		{0, "test", "test.min"},
		{1, "test", "test.max"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			assert.Equal(t, test.expected, quantileToName(test.basename, test.quantile))
		})
	}
}

func TestQuantileToNameSuffix(t *testing.T) {
	tests := []struct {
		quantile float64
		basename string
		expected string
	}{
		{0.5, "test", "test.quantile.50"},
		{0.9, "test", "test.quantile.90"},
		{0.99, "test", "test.quantile.99"},
		{0.999, "test", "test.quantile.99_9"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			assert.Equal(t, test.expected, quantileToNameSuffix(test.basename, test.quantile))
		})
	}
}

func TestCreateCountMetric(t *testing.T) {
	ilm := pmetric.NewScopeMetrics()

	metric := pmetric.NewMetric()
	metric.SetName("test.metric")
	metric.SetDescription("test description")
	metric.SetUnit("1")
	summary := metric.SetEmptySummary()
	summaryDataPoints := summary.DataPoints()
	summaryDataPoints.AppendEmpty().SetCount(10)
	summaryDataPoints.At(0).SetTimestamp(100)
	summaryDataPoints.At(0).SetStartTimestamp(0)
	summaryDataPoints.AppendEmpty().SetCount(20)
	summaryDataPoints.At(1).SetTimestamp(1000)
	summaryDataPoints.At(1).SetStartTimestamp(900)

	createCountMetric(metric, ilm)

	assert.Equal(t, 1, ilm.Metrics().Len())
	assert.Equal(t, "test.metric.count", ilm.Metrics().At(0).Name())
	assert.Equal(t, "test description", ilm.Metrics().At(0).Description())
	assert.Equal(t, "1", ilm.Metrics().At(0).Unit())
	assert.Equal(t, 2, ilm.Metrics().At(0).Sum().DataPoints().Len())
	assert.Equal(t, int64(10), ilm.Metrics().At(0).Sum().DataPoints().At(0).IntValue())
	assert.Equal(t, int64(20), ilm.Metrics().At(0).Sum().DataPoints().At(1).IntValue())
	assert.Equal(t, pmetric.AggregationTemporalityDelta, ilm.Metrics().At(0).Sum().AggregationTemporality())
	assert.Equal(t, false, ilm.Metrics().At(0).Sum().IsMonotonic())
	assert.Equal(t, pcommon.Timestamp(100), ilm.Metrics().At(0).Sum().DataPoints().At(0).StartTimestamp())
	assert.Equal(t, pcommon.Timestamp(100), ilm.Metrics().At(0).Sum().DataPoints().At(0).Timestamp())
}

func TestCreateSumMetric(t *testing.T) {
	ilm := pmetric.NewScopeMetrics()

	metric := pmetric.NewMetric()
	metric.SetName("test.metric")
	summary := metric.SetEmptySummary()
	summaryDataPoints := summary.DataPoints()
	summaryDataPoints.AppendEmpty().SetSum(10)
	summaryDataPoints.AppendEmpty().SetSum(20)

	createSumMetric(metric, ilm)

	assert.Equal(t, 1, ilm.Metrics().Len())
	assert.Equal(t, "test.metric.sum", ilm.Metrics().At(0).Name())
	assert.Equal(t, 2, ilm.Metrics().At(0).Gauge().DataPoints().Len())
	assert.Equal(t, 10.0, ilm.Metrics().At(0).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 20.0, ilm.Metrics().At(0).Gauge().DataPoints().At(1).DoubleValue())
}

func TestCreateQuantileMetrics(t *testing.T) {
	ilm := pmetric.NewScopeMetrics()

	metric := pmetric.NewMetric()
	metric.SetName("test.metric")
	summary := metric.SetEmptySummary()
	summaryDataPoints := summary.DataPoints()
	dp := summaryDataPoints.AppendEmpty()
	dp.Attributes().PutStr("key", "value")
	dp.SetTimestamp(100)
	dp.SetStartTimestamp(0)
	q := dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.5)
	q.SetValue(50)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.9)
	q.SetValue(90)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.99)
	q.SetValue(99)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.999)
	q.SetValue(99.9)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0)
	q.SetValue(1)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(1)
	q.SetValue(100)

	dp = summaryDataPoints.AppendEmpty()
	dp.Attributes().PutStr("key2", "value2")
	dp.SetTimestamp(200)
	dp.SetStartTimestamp(100)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.5)
	q.SetValue(150)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.9)
	q.SetValue(190)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.99)
	q.SetValue(199)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.999)
	q.SetValue(199.9)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0)
	q.SetValue(2)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(1)
	q.SetValue(200)

	createQuantileMetrics(metric, ilm)

	assert.Equal(t, 6, ilm.Metrics().Len())
	assert.Equal(t, "test.metric.quantile.50", ilm.Metrics().At(0).Name())
	assert.Equal(t, "test.metric.quantile.90", ilm.Metrics().At(1).Name())
	assert.Equal(t, "test.metric.quantile.99", ilm.Metrics().At(2).Name())
	assert.Equal(t, "test.metric.quantile.99_9", ilm.Metrics().At(3).Name())
	assert.Equal(t, "test.metric.min", ilm.Metrics().At(4).Name())
	assert.Equal(t, "test.metric.max", ilm.Metrics().At(5).Name())

	assert.Equal(t, 2, ilm.Metrics().At(0).Gauge().DataPoints().Len())
	assert.Equal(t, 50.0, ilm.Metrics().At(0).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 150.0, ilm.Metrics().At(0).Gauge().DataPoints().At(1).DoubleValue())
	v, found := ilm.Metrics().At(0).Gauge().DataPoints().At(0).Attributes().Get("key")
	assert.True(t, found)
	assert.Equal(t, "value", v.AsString())
	v, found = ilm.Metrics().At(0).Gauge().DataPoints().At(1).Attributes().Get("key2")
	assert.True(t, found)
	assert.Equal(t, "value2", v.AsString())
	assert.Equal(t, pcommon.Timestamp(100), ilm.Metrics().At(0).Gauge().DataPoints().At(0).StartTimestamp())
	assert.Equal(t, pcommon.Timestamp(100), ilm.Metrics().At(0).Gauge().DataPoints().At(0).Timestamp())

	assert.Equal(t, 2, ilm.Metrics().At(1).Gauge().DataPoints().Len())
	assert.Equal(t, 90.0, ilm.Metrics().At(1).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 190.0, ilm.Metrics().At(1).Gauge().DataPoints().At(1).DoubleValue())
	v, found = ilm.Metrics().At(1).Gauge().DataPoints().At(0).Attributes().Get("key")
	assert.True(t, found)
	assert.Equal(t, "value", v.AsString())
	v, found = ilm.Metrics().At(1).Gauge().DataPoints().At(1).Attributes().Get("key2")
	assert.True(t, found)
	assert.Equal(t, "value2", v.AsString())

	assert.Equal(t, 2, ilm.Metrics().At(2).Gauge().DataPoints().Len())
	assert.Equal(t, 99.0, ilm.Metrics().At(2).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 199.0, ilm.Metrics().At(2).Gauge().DataPoints().At(1).DoubleValue())
	v, found = ilm.Metrics().At(2).Gauge().DataPoints().At(0).Attributes().Get("key")
	assert.True(t, found)
	assert.Equal(t, "value", v.AsString())
	v, found = ilm.Metrics().At(2).Gauge().DataPoints().At(1).Attributes().Get("key2")
	assert.True(t, found)
	assert.Equal(t, "value2", v.AsString())

	assert.Equal(t, 2, ilm.Metrics().At(3).Gauge().DataPoints().Len())
	assert.Equal(t, 99.9, ilm.Metrics().At(3).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 199.9, ilm.Metrics().At(3).Gauge().DataPoints().At(1).DoubleValue())
	v, found = ilm.Metrics().At(3).Gauge().DataPoints().At(0).Attributes().Get("key")
	assert.True(t, found)
	assert.Equal(t, "value", v.AsString())
	v, found = ilm.Metrics().At(3).Gauge().DataPoints().At(1).Attributes().Get("key2")
	assert.True(t, found)
	assert.Equal(t, "value2", v.AsString())

	assert.Equal(t, 2, ilm.Metrics().At(4).Gauge().DataPoints().Len())
	assert.Equal(t, 1.0, ilm.Metrics().At(4).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 2.0, ilm.Metrics().At(4).Gauge().DataPoints().At(1).DoubleValue())
	v, found = ilm.Metrics().At(4).Gauge().DataPoints().At(0).Attributes().Get("key")
	assert.True(t, found)
	assert.Equal(t, "value", v.AsString())
	v, found = ilm.Metrics().At(4).Gauge().DataPoints().At(1).Attributes().Get("key2")
	assert.True(t, found)
	assert.Equal(t, "value2", v.AsString())

	assert.Equal(t, 2, ilm.Metrics().At(5).Gauge().DataPoints().Len())
	assert.Equal(t, 100.0, ilm.Metrics().At(5).Gauge().DataPoints().At(0).DoubleValue())
	assert.Equal(t, 200.0, ilm.Metrics().At(5).Gauge().DataPoints().At(1).DoubleValue())
	v, found = ilm.Metrics().At(5).Gauge().DataPoints().At(0).Attributes().Get("key")
	assert.True(t, found)
	assert.Equal(t, "value", v.AsString())
	v, found = ilm.Metrics().At(5).Gauge().DataPoints().At(1).Attributes().Get("key2")
	assert.True(t, found)
	assert.Equal(t, "value2", v.AsString())
}

func TestConsumeMetrics_empty_input(t *testing.T) {
	meter := metric.NewMeterProvider()
	ic, _ := meter.Meter("test").Int64Counter("dummy")

	e := &summarysplit{
		logger: zap.NewNop(),
		input:  ic,
		output: ic,
	}

	ctx := context.Background()

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty()
	md.ResourceMetrics().At(0).ScopeMetrics().AppendEmpty()
	md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty().SetEmptyGauge()

	newMD, err := e.ConsumeMetrics(ctx, pmetric.NewMetrics())
	assert.NoError(t, err)
	assert.Equal(t, 0, newMD.ResourceMetrics().Len())
}

func TestConsumeMetrics_no_summary_data_points(t *testing.T) {
	meter := metric.NewMeterProvider()
	ic, _ := meter.Meter("test").Int64Counter("dummy")

	e := &summarysplit{
		logger: zap.NewNop(),
		input:  ic,
		output: ic,
	}

	ctx := context.Background()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ilm := rm.ScopeMetrics().AppendEmpty()
	metric := ilm.Metrics().AppendEmpty()
	metric.SetEmptyGauge()
	metric.SetName("dummy")

	newMD, err := e.ConsumeMetrics(ctx, md)
	assert.NoError(t, err)
	assert.Equal(t, 1, newMD.ResourceMetrics().Len())
	assert.Equal(t, 1, newMD.ResourceMetrics().At(0).ScopeMetrics().Len())
	assert.Equal(t, 1, newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len())
	assert.Equal(t, "dummy", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	assert.Equal(t, pmetric.MetricTypeGauge, newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Type())
}

func TestConsumeMetrics_with_summary_data_points(t *testing.T) {
	meter := metric.NewMeterProvider()
	ic, _ := meter.Meter("test").Int64Counter("dummy")

	e := &summarysplit{
		logger: zap.NewNop(),
		input:  ic,
		output: ic,
	}

	ctx := context.Background()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ilm := rm.ScopeMetrics().AppendEmpty()
	metric := ilm.Metrics().AppendEmpty()
	metric.SetName("test.metric")
	summary := metric.SetEmptySummary()
	summaryDataPoints := summary.DataPoints()
	dp := summaryDataPoints.AppendEmpty()
	dp.SetCount(10)
	dp.SetSum(100)
	q := dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.5)
	q.SetValue(50)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.999)
	q.SetValue(99.9)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(0)
	q.SetValue(1)
	q = dp.QuantileValues().AppendEmpty()
	q.SetQuantile(1)
	q.SetValue(100)

	newMD, err := e.ConsumeMetrics(ctx, md)
	require.NoError(t, err)
	require.Equal(t, 1, newMD.ResourceMetrics().Len())
	require.Equal(t, 1, newMD.ResourceMetrics().At(0).ScopeMetrics().Len())
	require.Equal(t, 6, newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len())
	assert.Equal(t, "test.metric.count", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	assert.Equal(t, "test.metric.sum", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(1).Name())
	assert.Equal(t, "test.metric.quantile.50", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(2).Name())
	assert.Equal(t, "test.metric.quantile.99_9", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(3).Name())
	assert.Equal(t, "test.metric.min", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(4).Name())
	assert.Equal(t, "test.metric.max", newMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(5).Name())
}
