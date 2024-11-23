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

package chqstatsprocessor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
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
	processor := &statsProc{
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
