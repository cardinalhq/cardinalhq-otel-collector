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

package chqstatsexporter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestGetBoolOrDefault(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutBool("key1", true)
	attr.PutBool("key1f", false)
	attr.PutStr("key2", "value2")

	tests := []struct {
		name     string
		attr     pcommon.Map
		key      string
		def      bool
		expected bool
	}{
		{"bool value exists and is true, default false", attr, "key1", false, true},
		{"bool value exists and is false, default true", attr, "key1f", true, false},
		{"string value exists, default false", attr, "key2", false, false},
		{"string value exists, default true", attr, "key2", true, true},
		{"key does not exist, default false", attr, "key3", false, false},
		{"key does not exist, value true", attr, "key3", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getBoolOrDefault(tt.attr, tt.key, tt.def)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestMetricBoolsToPhase(t *testing.T) {
	tests := []struct {
		name           string
		wasAggregated  bool
		isAggregation  bool
		expectedResult chqpb.Phase
	}{
		{"wasAggregated=true, isAggregation=false", true, false, chqpb.Phase_AGGREGATED},
		{"wasAggregated=false, isAggregation=true", false, true, chqpb.Phase_AGGREGATION_OUTPUT},
		{"wasAggregated=false, isAggregation=false", false, false, chqpb.Phase_PASSTHROUGH},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := metricBoolsToPhase(tt.wasAggregated, tt.isAggregation)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestPostMetricStats(t *testing.T) {
	// Create a mock server to handle the HTTP request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))
		assert.Equal(t, "YOUR_API_KEY", r.Header.Get("x-cardinalhq-api-key"))

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
		assert.Equal(t, float64(4), report.Stats[0].CardinalityEstimate)
		assert.Equal(t, []byte{10, 20, 30}, report.Stats[0].Hll)

		w.WriteHeader(http.StatusOK)
	}))

	defer server.Close()

	// Create a statsExporter instance with the mock server's URL and API key
	exporter := &statsExporter{
		config: &Config{
			ClientConfig: confighttp.ClientConfig{
				Endpoint: server.URL,
			},
			APIKey: "YOUR_API_KEY",
		},
		httpClient: server.Client(),
		logger:     zap.NewNop(),
	}

	// Create a mock MetricStatsReport
	report := &chqpb.MetricStatsReport{
		SubmittedAt: 1234567890,
		Stats: []*chqpb.MetricStats{
			{
				MetricName:          "metric_name",
				TagName:             "tag_name",
				ServiceName:         "service_name",
				Phase:               2,
				Count:               3,
				CardinalityEstimate: 4,
				Hll:                 []byte{10, 20, 30},
			},
		},
	}

	// Call the postMetricStats function
	err := exporter.postMetricStats(context.Background(), report)

	// Verify that no error occurred
	assert.NoError(t, err)
}
