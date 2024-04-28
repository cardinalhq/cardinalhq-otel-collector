package chqdecoratorprocessor

import (
	"testing"
	"time"

	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/sampler"
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

func (m *MockAggregator[T]) MatchAndAdd(t *time.Time, buckets []T, values []T, ty sampler.AggregationType, name string, metadata map[string]string, rattr pcommon.Map, iattr pcommon.Map, mattr pcommon.Map) (string, error) {
	return "bob", nil
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
