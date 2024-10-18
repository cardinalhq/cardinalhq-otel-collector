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

package ottl

import (
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestSplitTag(t *testing.T) {
	tests := []struct {
		name          string
		tag           string
		expectedScope string
		expectedName  string
	}{
		{"scope.name", "scope.name", "scope", "name"},
		{"invalidTag", "invalidtag", "", ""},
		{".foo", ".foo", "", ""},
		{"foo.", "foo.", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope, name := SplitTag(tt.tag)
			if scope != tt.expectedScope {
				t.Errorf("Expected scope %s, but got %s", tt.expectedScope, scope)
			}
			if name != tt.expectedName {
				t.Errorf("Expected name %s, but got %s", tt.expectedName, name)
			}
		})
	}
}

func TestAttrsToMap(t *testing.T) {
	attrs := map[string]pcommon.Map{
		"scope1": pcommon.NewMap(),
		"scope2": pcommon.NewMap(),
	}

	attrs["scope1"].PutStr("name1", "value1")
	attrs["scope1"].PutInt("name2", 123)
	attrs["scope2"].PutBool("name3", true)

	expected := map[string]string{
		"scope1.name1": "value1",
		"scope1.name2": "123",
		"scope2.name3": "true",
	}

	result := attrsToMap(attrs)

	if len(result) != len(expected) {
		t.Errorf("Expected %d attributes, but got %d", len(expected), len(result))
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("Expected value %s for attribute %s, but got %s", v, k, result[k])
		}
	}
}

func TestTimebox(t *testing.T) {
	tests := []struct {
		name     string
		t        time.Time
		interval int64
		want     int64
	}{
		{
			name:     "time within interval",
			t:        time.UnixMilli(1641024001231),
			interval: 1000,
			want:     1641024001000,
		},
		{
			name:     "time at interval boundary start",
			t:        time.UnixMilli(1641024000000),
			interval: 1000,
			want:     1641024000000,
		},
		{
			name:     "time at interval boundary end",
			t:        time.UnixMilli(1641024000999),
			interval: 1000,
			want:     1641024000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := timebox(tt.t, tt.interval)
			assert.Equal(t, tt.want, actual)
		})
	}
}

func TestMatchAndAdd_AverageAfterDroppingDimension(t *testing.T) {
	statements := []ContextStatement{
		{
			Context:    "datapoint",
			Conditions: []string{},
			Statements: []string{
				`set(attributes["movieId"], "_aggregated")`,
				`set(attributes["_cardinalhq.aggregate"], true)`,
			},
		},
	}

	// Parse transformations
	instruction := Instruction{
		VendorId:   "vendor1",
		Statements: statements,
	}
	transformations, err := ParseTransformations(instruction, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, transformations)

	// Initialize metric aggregator
	m := NewMetricAggregatorImpl[float64](10)

	// Create ResourceMetrics
	rm := pmetric.NewResourceMetrics()

	// Create ScopeMetrics
	scopeMetrics := rm.ScopeMetrics().AppendEmpty()
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("metric1")
	metric.SetEmptyGauge()

	// Create the first DataPoint (movieId1)
	dp1 := metric.Gauge().DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.Timestamp(1641024001231000000)) // Corresponds to time.UnixMilli(1641024001231)
	dp1.Attributes().PutStr("movieId", "movieId1")
	dp1.SetDoubleValue(1.0)

	// Create the second DataPoint (movieId2)
	dp2 := metric.Gauge().DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.Timestamp(1641024001231000000)) // Same timestamp as dp1
	dp2.Attributes().PutStr("movieId", "movieId2")
	dp1.SetDoubleValue(2.0)

	// Apply transformations to both data points
	tc1 := ottldatapoint.NewTransformContext(dp1, metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), rm.Resource(), scopeMetrics, rm)
	tc2 := ottldatapoint.NewTransformContext(dp2, metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), rm.Resource(), scopeMetrics, rm)
	transformations.ExecuteDatapointTransforms(nil, tc1, "vendorId", pcommon.NewSlice())
	transformations.ExecuteDatapointTransforms(nil, tc2, "vendorId", pcommon.NewSlice())

	// At this point, both data points should have their "movieId" attribute dropped.

	ttime := time.UnixMilli(1641024001231)
	tbox := timebox(ttime, 10)

	// Call MatchAndAdd for both data points (which have had movieId dropped)
	rattr := rm.Resource().Attributes()
	sattr := scopeMetrics.Scope().Attributes()
	dp1Attributes := dp1.Attributes()
	dp2Attributes := dp2.Attributes()

	buckets := []float64{1}

	_, err = m.MatchAndAdd(&ttime, buckets, []float64{1.0}, AggregationTypeAvg, "metric1", nil, rattr, sattr, dp1Attributes)
	require.Nil(t, err)

	_, err = m.MatchAndAdd(&ttime, buckets, []float64{2.0}, AggregationTypeAvg, "metric1", nil, rattr, sattr, dp2Attributes)
	require.Nil(t, err)

	expectedAverage := 1.5

	assert.Equal(t, 1, len(m.sets))
	_, found := m.sets[tbox]
	require.True(t, found)
	aggregations := m.sets[tbox].Aggregations
	assert.Equal(t, 1, len(aggregations))

	for _, v := range aggregations {
		assert.Equal(t, "metric1", v.Name())
		assert.Equal(t, AggregationTypeAvg, v.AggregationType())
		assert.Equal(t, expectedAverage, v.Value()[0])
		assert.Equal(t, uint64(2), v.Count()) // Both data points are aggregated
	}
}

func TestNowTime(t *testing.T) {
	tt := time.Now()
	actual := nowtime(&tt)
	assert.Equal(t, tt, *actual)

	actual = nowtime(nil)
	assert.True(t, time.Since(*actual) < time.Second)
}
