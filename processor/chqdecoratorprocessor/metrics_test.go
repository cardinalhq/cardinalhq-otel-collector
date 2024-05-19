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

package chqdecoratorprocessor

import (
	"testing"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/sampler"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var _ sampler.MetricAggregator[float64] = &MockAggregator[float64]{}

type MockAggregator[T float64] struct {
}

func (m *MockAggregator[T]) Configure(config []sampler.AggregatorConfig) {
}

func (m *MockAggregator[T]) Aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) float64 {
	return 0
}

func (m *MockAggregator[T]) Emit(t time.Time) map[int64]*sampler.AggregationSet[T] {
	return nil
}

func (m *MockAggregator[T]) MatchAndAdd(t *time.Time, buckets []T, values []T, ty sampler.AggregationType, name string, metadata map[string]string, rattr pcommon.Map, iattr pcommon.Map, mattr pcommon.Map) (*sampler.AggregatorConfig, error) {
	return &sampler.AggregatorConfig{Id: "bob"}, nil
}

func TestAggregateGauge(t *testing.T) {
	rms := pmetric.NewResourceMetrics()
	ils := rms.ScopeMetrics().AppendEmpty()
	metric := ils.Metrics().AppendEmpty()
	metric.SetName("test")

	metric.SetEmptyGauge()
	dp := metric.Gauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.0)
	dp.Attributes().PutStr("foo", "bar")
	dp = metric.Gauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(2.0)
	dp.Attributes().PutStr("foo", "bar")
	dp = metric.Gauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(3.0)
	dp.Attributes().PutStr("foo", "bar")

	mp := &metricProcessor{
		aggregatorF: &MockAggregator[float64]{},
	}

	aggregated := mp.aggregateGauge(rms, ils, metric)

	// Verify the result
	assert.Equal(t, int64(3), aggregated)

	// Verify that attributes were modified
	for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)
		attr := dp.Attributes()
		filtered, found := attr.Get("_cardinalhq.filtered")
		assert.True(t, found)
		assert.Equal(t, "true", filtered.AsString())
	}
}
func TestSetResourceMetadata(t *testing.T) {
	res := pmetric.NewResourceMetrics()

	setResourceMetadata(res, "schemaurl", "schemaurl")
	assert.Equal(t, "schemaurl", res.SchemaUrl(), "schemaurl")
}

func TestSetInstrumentationMetadata(t *testing.T) {
	res := pmetric.NewResourceMetrics()
	ils := res.ScopeMetrics().AppendEmpty()

	setInstrumentationMetadata(ils, "schemaurl", "schemaurl")
	assert.Equal(t, "schemaurl", ils.SchemaUrl(), "schemaurl")

	setInstrumentationMetadata(ils, "version", "alice-1.0.2")
	assert.Equal(t, "alice-1.0.2", ils.Scope().Version(), "version")

	setInstrumentationMetadata(ils, "name", "alice")
	assert.Equal(t, "alice", ils.Scope().Name(), "name")
}

func TestSetMetricMetadata(t *testing.T) {
	res := pmetric.NewResourceMetrics()
	ils := res.ScopeMetrics().AppendEmpty()
	metric := ils.Metrics().AppendEmpty()

	setMetricMetadata(metric, "name", "alice.one")
	assert.Equal(t, "alice.one", metric.Name(), "name")

	setMetricMetadata(metric, "description", "Alice's first metric")
	assert.Equal(t, "Alice's first metric", metric.Description(), "description")

	setMetricMetadata(metric, "unit", "alice")
	assert.Equal(t, "alice", metric.Unit(), "unit")
}

func TestSetMetricMetadataAggregationTemporality(t *testing.T) {
	res := pmetric.NewResourceMetrics()
	ils := res.ScopeMetrics().AppendEmpty()
	metric := ils.Metrics().AppendEmpty()
	metric.SetEmptySum()

	setMetricMetadata(metric, "aggregationtemporality", "delta")
	assert.Equal(t, pmetric.AggregationTemporalityDelta, metric.Sum().AggregationTemporality(), "aggregationtemporality")

	setMetricMetadata(metric, "aggregationtemporality", "cumulative")
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, metric.Sum().AggregationTemporality(), "aggregationtemporality")
}

func TestSetMetricMetadataMonotonic(t *testing.T) {
	res := pmetric.NewResourceMetrics()
	ils := res.ScopeMetrics().AppendEmpty()
	metric := ils.Metrics().AppendEmpty()
	metric.SetEmptySum()

	setMetricMetadata(metric, "ismonotonic", "true")
	assert.Equal(t, true, metric.Sum().IsMonotonic(), "ismonotonic")

	setMetricMetadata(metric, "ismonotonic", "false")
	assert.Equal(t, false, metric.Sum().IsMonotonic(), "ismonotonic")
}

func TestNotSumWontCrash(t *testing.T) {
	res := pmetric.NewResourceMetrics()
	ils := res.ScopeMetrics().AppendEmpty()
	metric := ils.Metrics().AppendEmpty()
	metric.SetEmptyGauge()

	setMetricMetadata(metric, "aggregationtemporality", "delta")
	setMetricMetadata(metric, "ismonotonic", "true")
}
