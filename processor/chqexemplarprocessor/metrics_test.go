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

package chqexemplarprocessor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/proto"
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
		assert.InEpsilon(t, float64(4), report.Stats[0].CardinalityEstimate, 0.0001)
		assert.Equal(t, []byte{10, 20, 30}, report.Stats[0].Hll)

		w.WriteHeader(http.StatusOK)
	}))

	defer server.Close()
}

func TestSingularExemplar(t *testing.T) {
	exemplarData, err := os.ReadFile("testdata/exemplar.json")
	assert.NoError(t, err, "Failed to read exemplar data")

	unmarshaller := pmetric.JSONUnmarshaler{}
	exemplar, err := unmarshaller.UnmarshalMetrics(exemplarData)
	require.NoError(t, err)
	assert.NotNil(t, exemplar)

	rm := exemplar.ResourceMetrics().At(0)
	sm := rm.ScopeMetrics().At(0)
	mm := sm.Metrics().At(0)

	for k := 0; k < sm.Metrics().Len(); k++ {
		m := sm.Metrics().At(k)
		e := toMetricExemplar(rm, sm, mm, m.Type())
		assert.True(t, e.ResourceMetrics().Len() == 1)
		assert.True(t, e.ResourceMetrics().At(0).ScopeMetrics().Len() == 1)
		assert.True(t, e.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len() == 1)
		assert.True(t, e.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().Len() == 1)
	}
}
