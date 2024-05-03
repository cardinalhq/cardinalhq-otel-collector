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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/trigram"
)

func TestValueToString(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{
			"nil",
			nil,
			"",
		},
		{
			"empty string",
			"",
			"",
		},
		{
			"string",
			"abc",
			"abc",
		},
		{
			"int",
			123,
			"123",
		},
		{
			"bool",
			true,
			"true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valueToString(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCalculateTID(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]any
		want int64
	}{
		{
			"all string tags",
			map[string]any{
				"tag1": "abc",
				"tag3": "xyz",
				"tag2": "hello",
			},
			trigram.JavaHashcode("abc:hello:xyz"),
		},
		{
			"int and string tags",
			map[string]any{
				"tag1": 999,
				"tag3": "xyz",
				"tag2": "hello",
			},
			trigram.JavaHashcode("999:hello:xyz"),
		},
		{
			"from live data test 1",
			map[string]any{
				"cluster_name$string":      "cardinalhq-dev-demo",
				"host$string":              "i-0a56936559d3bc37d",
				"kube_cluster_name$string": "cardinalhq-dev-demo",
				"telemetry_type$string":    "metrics",
				"provider$string":          "datadog",
				"kube_node$string":         "ip-10-0-0-58.us-east-2.compute.internal",
				"metric_type$string":       "gauge",
				"name$string":              "n_o_i_n_d_e_x.datadog.cluster_agent.payload.dropped",
			},
			7245350178181118928,
		},
		{
			"ignores timestamp and value",
			map[string]any{
				"_cardinalhq.timestamp":    int64(1712667843000),
				"_cardinalhq.value":        float64(0.0),
				"cluster_name$string":      "cardinalhq-dev-demo",
				"host$string":              "i-0a56936559d3bc37d",
				"kube_cluster_name$string": "cardinalhq-dev-demo",
				"telemetry_type$string":    "metrics",
				"provider$string":          "datadog",
				"kube_node$string":         "ip-10-0-0-58.us-east-2.compute.internal",
				"metric_type$string":       "gauge",
				"name$string":              "n_o_i_n_d_e_x.datadog.cluster_agent.payload.dropped",
			},
			7245350178181118928,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateTID(tt.tags)
			assert.Equal(t, tt.want, got)
		})
	}
}
