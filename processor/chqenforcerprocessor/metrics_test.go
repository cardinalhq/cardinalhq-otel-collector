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

package chqenforcerprocessor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
)

func TestPostMetricStats(t *testing.T) {
	// Create a mock server to handle the HTTP request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)

		report := &chqpb.MetricStatsReport{}
		assert.NoError(t, proto.Unmarshal(body, report))

		assert.Equal(t, int64(1234567890), report.SubmittedAt)

		assert.Len(t, report.Stats, 1)
		assert.Equal(t, "metric_name", report.Stats[0].MetricName)
		assert.Equal(t, "tag_name", report.Stats[0].TagName)
		assert.Equal(t, "service_name", report.Stats[0].ServiceName)
		assert.Equal(t, chqpb.Phase(2), report.Stats[0].Phase)
		assert.Equal(t, int64(3), report.Stats[0].Count)
		//assert.Equal(t, float64(4), report.Stats[0].CardinalityEstimate)
		assert.Equal(t, []byte{10, 20, 30}, report.Stats[0].Hll)

		w.WriteHeader(http.StatusOK)
	}))

	defer server.Close()

	// Create a statsExporter instance with the mock server's URL and API key
	processor := &chqEnforcer{
		config: &Config{
			Statistics: StatisticsConfig{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: server.URL,
				},
			},
		},
		httpClient: server.Client(),
		logger:     zap.NewNop(),
	}

	// Create a mock MetricStatsReport
	report := &chqpb.MetricStatsReport{
		SubmittedAt: 1234567890,
		Stats: []*chqpb.MetricStats{
			{
				MetricName:  "metric_name",
				TagName:     "tag_name",
				ServiceName: "service_name",
				Phase:       2,
				Count:       3,
				//CardinalityEstimate: 4,
				Hll: []byte{10, 20, 30},
			},
		},
	}

	// Call the postMetricStats function
	err := processor.postMetricStats(context.Background(), report)

	// Verify that no error occurred
	assert.NoError(t, err)
}

var _ ottl.MetricAggregator[float64] = &MockAggregator[float64]{}

type MockAggregator[T float64] struct {
}

func (m *MockAggregator[T]) Configure(config ottl.SamplerConfig, vendor string) {
}

func (m *MockAggregator[T]) Aggregate(rms pmetric.ResourceMetrics, ils pmetric.ScopeMetrics, metric pmetric.Metric) float64 {
	return 0
}

func (m *MockAggregator[T]) Emit(t time.Time) map[int64]*ottl.AggregationSet[T] {
	return nil
}

func (m *MockAggregator[T]) MatchAndAdd(t *time.Time, buckets []T, values []T, ty ottl.AggregationType, name string, metadata map[string]string, rattr pcommon.Map, iattr pcommon.Map, mattr pcommon.Map) (bool, error) {
	return true, nil
}

func (m *MockAggregator[T]) HasRules() bool {
	return false
}

func TestAggregateGauge(t *testing.T) {
	rms := pmetric.NewResourceMetrics()
	ils := rms.ScopeMetrics().AppendEmpty()
	metric := ils.Metrics().AppendEmpty()
	metric.SetName("test")

	metric.SetEmptyGauge()
	dp1 := metric.Gauge().DataPoints().AppendEmpty()
	dp1.SetDoubleValue(1.0)
	dp1.Attributes().PutStr("foo", "bar")
	dp2 := metric.Gauge().DataPoints().AppendEmpty()
	dp2.SetDoubleValue(2.0)
	dp2.Attributes().PutStr("foo", "bar")
	dp3 := metric.Gauge().DataPoints().AppendEmpty()
	dp3.SetDoubleValue(3.0)
	dp3.Attributes().PutStr("foo", "bar")

	mp := &chqEnforcer{
		aggregatorF: &MockAggregator[float64]{},
	}

	aggregated := mp.aggregateGaugeDatapoint(rms, ils, metric, dp1)
	assert.True(t, aggregated)
	aggregated = mp.aggregateGaugeDatapoint(rms, ils, metric, dp2)
	assert.True(t, aggregated)
	aggregated = mp.aggregateGaugeDatapoint(rms, ils, metric, dp3)
	assert.True(t, aggregated)
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
