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
	"testing"

	"github.com/apache/datasketches-go/hll"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
)

func TestMetricStat_Key(t *testing.T) {
	m := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		Phase:       chqpb.Phase_PASSTHROUGH,
		Count:       0,
	}

	assert.Equal(t, uint64(0xf4f907b19b714ec7), m.Key())
}

func TestMetricStat_Matches(t *testing.T) {
	m := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		Phase:       chqpb.Phase_PASSTHROUGH,
		Count:       0,
	}

	other := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		Phase:       chqpb.Phase_PASSTHROUGH,
		Count:       0,
	}

	assert.True(t, m.Matches(other))
}

func TestMetricStat_Initialize(t *testing.T) {
	m := &MetricStat{
		MetricName:  "test_metric",
		TagName:     "test_tag",
		ServiceName: "test_service",
		Phase:       chqpb.Phase_PASSTHROUGH,
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
		Phase:       chqpb.Phase_PASSTHROUGH,
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
