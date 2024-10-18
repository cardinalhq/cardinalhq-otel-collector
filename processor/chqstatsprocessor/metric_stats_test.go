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
	"testing"

	"github.com/apache/datasketches-go/hll"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

func TestMetricStat_Key(t *testing.T) {
	m := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		VendorID:    "test_vendor",
		Phase:       chqpb.Phase_PRE,
		Count:       0,
	}

	assert.Equal(t, uint64(0x24f43c4422ea113e), m.Key())
}

type testMetricStat struct{}

func (t *testMetricStat) Matches(other stats.StatsObject) bool {
	return true
}

func (t *testMetricStat) Initialize() error {
	return nil
}

func (t *testMetricStat) Increment(tag string, value int, timestamp int64) error {
	return nil
}

func (t *testMetricStat) Key() uint64 {
	return 0
}

func TestMetricStat_Matches(t *testing.T) {
	tests := []struct {
		name     string
		m        *MetricStat
		other    stats.StatsObject
		expected bool
	}{
		{
			"identical",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			true,
		},
		{
			"different type",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&testMetricStat{},
			false,
		},
		{
			"different metric name",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric2",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			false,
		},
		{
			"different tag name",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag2",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			false,
		},
		{
			"different service name",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service2",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			false,
		},
		{
			"different vendor id",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor2",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			false,
		},
		{
			"different phase",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_POST,
				Count:       0,
			},
			false,
		},
		{
			"different count",
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       0,
			},
			&MetricStat{
				MetricName:  "test_metric",
				TagName:     "test_tag",
				ServiceName: "test_service",
				VendorID:    "test_vendor",
				Phase:       chqpb.Phase_PRE,
				Count:       1,
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.m.Matches(tt.other))
		})
	}
}

func TestMetricStat_Initialize(t *testing.T) {
	m := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		VendorID:    "test_vendor",
		Phase:       chqpb.Phase_PRE,
		Count:       0,
	}

	err := m.Initialize()
	assert.NoError(t, err)
	assert.NotNil(t, m.HLL)
}

func TestMetricStat_Increment(t *testing.T) {
	m := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		VendorID:    "test_vendor",
		Phase:       chqpb.Phase_PRE,
		Count:       0,
	}

	hll, err := hll.NewHllSketchWithDefault()
	assert.NoError(t, err)
	m.HLL = hll
	err = m.Increment("test_tag", 5, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), m.Count)

	err = m.Increment("test_tag", 10, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), m.Count)
}
