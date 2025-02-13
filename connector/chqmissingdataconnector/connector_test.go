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

package chqmissingdataconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestPrefixedName(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		input    string
		expected string
	}{
		{
			name:     "No prefix",
			config:   &Config{NamePrefix: ""},
			input:    "metricName",
			expected: "metricName",
		},
		{
			name:     "With prefix",
			config:   &Config{NamePrefix: "prefix_"},
			input:    "metricName",
			expected: "prefix_metricName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{config: tt.config}
			result := md.prefixedName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
func TestBuildMetrics(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		emitList []Stamp
		verify   func(t *testing.T, metrics pmetric.Metrics)
	}{
		{
			name:     "Empty emit list",
			config:   &Config{NamePrefix: ""},
			emitList: []Stamp{},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 0, metrics.DataPointCount())
			},
		},
		{
			name:   "Single stamp",
			config: &Config{NamePrefix: "prefix_"},
			emitList: []Stamp{
				{
					MetricName:         "metricName",
					ResourceAttributes: pcommon.NewMap(),
					LastSeen:           time.Now().Add(-time.Minute),
				},
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 1, metrics.DataPointCount())
				rm := metrics.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				metric := sm.Metrics().At(0)
				assert.Equal(t, "prefix_metricName", metric.Name())
				assert.Equal(t, "Missing data indicator", metric.Description())
				assert.Equal(t, "s", metric.Unit())
			},
		},
		{
			name:   "Multiple stamps",
			config: &Config{NamePrefix: "prefix_"},
			emitList: []Stamp{
				{
					MetricName:         "metricName1",
					ResourceAttributes: pcommon.NewMap(),
					LastSeen:           time.Now().Add(-time.Minute),
				},
				{
					MetricName:         "metricName2",
					ResourceAttributes: pcommon.NewMap(),
					LastSeen:           time.Now().Add(-2 * time.Minute),
				},
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 2, metrics.DataPointCount())
				rm := metrics.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				metric1 := sm.Metrics().At(0)
				metric2 := sm.Metrics().At(1)
				assert.Equal(t, "prefix_metricName1", metric1.Name())
				assert.Equal(t, "prefix_metricName2", metric2.Name())
			},
		},
		{
			name:   "Multiple stamps with same resource",
			config: &Config{NamePrefix: "prefix_"},
			emitList: []Stamp{
				{
					MetricName:         "metricName1",
					ResourceAttributes: pcommon.NewMap(),
					LastSeen:           time.Now().Add(-time.Minute),
				},
				{
					MetricName:         "metricName2",
					ResourceAttributes: pcommon.NewMap(),
					LastSeen:           time.Now().Add(-2 * time.Minute),
				},
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 2, metrics.DataPointCount())
				rm := metrics.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				metric1 := sm.Metrics().At(0)
				metric2 := sm.Metrics().At(1)
				assert.Equal(t, "prefix_metricName1", metric1.Name())
				assert.Equal(t, "prefix_metricName2", metric2.Name())
			},
		},
		{
			name:   "Multiple stamps with different resources",
			config: &Config{NamePrefix: "prefix_"},
			emitList: []Stamp{
				{
					MetricName: "metricName1",
					ResourceAttributes: func() pcommon.Map {
						m := pcommon.NewMap()
						m.PutStr("key1", "value1")
						return m
					}(),
					LastSeen: time.Now().Add(-time.Minute),
				},
				{
					MetricName: "metricName2",
					ResourceAttributes: func() pcommon.Map {
						m := pcommon.NewMap()
						m.PutStr("key2", "value2")
						return m
					}(),
					LastSeen: time.Now().Add(-2 * time.Minute),
				},
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 2, metrics.DataPointCount())
				rm1 := metrics.ResourceMetrics().At(0)
				rm2 := metrics.ResourceMetrics().At(1)
				sm1 := rm1.ScopeMetrics().At(0)
				sm2 := rm2.ScopeMetrics().At(0)
				metric1 := sm1.Metrics().At(0)
				metric2 := sm2.Metrics().At(0)
				assert.Equal(t, "prefix_metricName1", metric1.Name())
				assert.Equal(t, "prefix_metricName2", metric2.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{config: tt.config}
			metrics := md.buildMetrics(tt.emitList)
			tt.verify(t, metrics)
		})
	}
}
