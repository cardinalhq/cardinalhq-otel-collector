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
)

func TestHashAttributes(t *testing.T) {
	tests := []struct {
		name     string
		attrs    pcommon.Map
		expected uint64
	}{
		{
			name:     "empty attributes",
			attrs:    pcommon.NewMap(),
			expected: 0xef46db3751d8e999,
		},
		{
			name: "single attribute",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(),
			expected: 0xd4be04e7ff35d3e5,
		},
		{
			name: "multiple attributes",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutStr("key2", "value2")
				return m
			}(),
			expected: 0x11e8f2d3a1f21d34,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashAttributes(tt.attrs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHashMetricNameAndAttributes(t *testing.T) {
	tests := []struct {
		name               string
		metricName         string
		resourceAttributes pcommon.Map
		dpAttributes       pcommon.Map
		expected           uint64
	}{
		{
			name:               "empty metric name and attributes",
			metricName:         "",
			resourceAttributes: pcommon.NewMap(),
			dpAttributes:       pcommon.NewMap(),
			expected:           0xef46db3751d8e999,
		},
		{
			name:               "metric name only",
			metricName:         "metric1",
			resourceAttributes: pcommon.NewMap(),
			dpAttributes:       pcommon.NewMap(),
			expected:           0x4e2e35d7ea4a9770,
		},
		{
			name:       "metric name with single attribute",
			metricName: "metric1",
			resourceAttributes: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(),
			dpAttributes: pcommon.NewMap(),
			expected:     0xbac8b377be93de3f,
		},
		{
			name:       "metric name with multiple attributes",
			metricName: "metric1",
			resourceAttributes: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutStr("key2", "value2")
				return m
			}(),
			dpAttributes: pcommon.NewMap(),
			expected:     0x84cd396ce2678d25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashMetric(tt.metricName, tt.resourceAttributes, tt.dpAttributes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewStamp(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		rattrs     pcommon.Map
		dpattrs    pcommon.Map
		timestamp  time.Time
	}{
		{
			name:       "empty attributes",
			metricName: "metric1",
			rattrs:     pcommon.NewMap(),
			dpattrs:    pcommon.NewMap(),
			timestamp:  time.Now(),
		},
		{
			name:       "single attribute",
			metricName: "metric1",
			rattrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(),
			dpattrs:   pcommon.NewMap(),
			timestamp: time.Now(),
		},
		{
			name:       "multiple attributes",
			metricName: "metric1",
			rattrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutStr("key2", "value2")
				return m
			}(),
			dpattrs:   pcommon.NewMap(),
			timestamp: time.Now(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stamp := NewStamp(tt.metricName, tt.rattrs, tt.dpattrs, tt.timestamp)
			assert.Equal(t, tt.timestamp, stamp.LastSeen)
			assert.Equal(t, tt.metricName, stamp.MetricName)
			assert.Equal(t, tt.rattrs.Len(), stamp.ResourceAttributes.Len())
			tt.rattrs.Range(func(k string, v pcommon.Value) bool {
				val, _ := stamp.ResourceAttributes.Get(k)
				assert.Equal(t, v.AsString(), val.AsString())
				return true
			})
		})
	}
}

func TestTouch(t *testing.T) {
	initialTime := time.Now()
	stamp := NewStamp("metric1", pcommon.NewMap(), pcommon.NewMap(), initialTime)

	newTime := initialTime.Add(1 * time.Hour)
	stamp.Touch(newTime)

	assert.Equal(t, newTime, stamp.LastSeen)
}

func TestIsExpired(t *testing.T) {
	tests := []struct {
		name      string
		lastSeen  time.Time
		checkTime time.Time
		ttl       time.Duration
		expected  bool
	}{
		{
			name:      "not expired",
			lastSeen:  time.Now().Add(-5 * time.Minute),
			checkTime: time.Now(),
			ttl:       10 * time.Minute,
			expected:  false,
		},
		{
			name:      "expired",
			lastSeen:  time.Now().Add(-15 * time.Minute),
			checkTime: time.Now(),
			ttl:       10 * time.Minute,
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stamp := &Stamp{
				LastSeen: tt.lastSeen,
			}
			result := stamp.IsExpired(tt.checkTime, tt.ttl)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStampHash(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		rattrs     pcommon.Map
		dpattrs    pcommon.Map
		expected   uint64
	}{
		{
			name:       "empty metric name and attributes",
			metricName: "",
			rattrs:     pcommon.NewMap(),
			dpattrs:    pcommon.NewMap(),
			expected:   0xef46db3751d8e999,
		},
		{
			name:       "metric name only",
			metricName: "metric1",
			rattrs:     pcommon.NewMap(),
			dpattrs:    pcommon.NewMap(),
			expected:   0x4e2e35d7ea4a9770,
		},
		{
			name:       "metric name with single attribute",
			metricName: "metric1",
			rattrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(),
			dpattrs:  pcommon.NewMap(),
			expected: 0xbac8b377be93de3f,
		},
		{
			name:       "metric name with multiple attributes",
			metricName: "metric1",
			rattrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutStr("key2", "value2")
				return m
			}(),
			dpattrs:  pcommon.NewMap(),
			expected: 0x84cd396ce2678d25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stamp := NewStamp(tt.metricName, tt.rattrs, tt.dpattrs, time.Now())
			result := stamp.Hash()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStampEquals(t *testing.T) {
	tests := []struct {
		name     string
		stamp1   *Stamp
		stamp2   *Stamp
		expected bool
	}{
		{
			name: "equal stamps",
			stamp1: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			stamp2: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			expected: true,
		},
		{
			name: "different metric names",
			stamp1: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			stamp2: NewStamp("metric2", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			expected: false,
		},
		{
			name: "different resource attributes",
			stamp1: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			stamp2: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key2", "value2")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			expected: false,
		},
		{
			name: "different datapoint attributes",
			stamp1: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			stamp2: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey2", "dpvalue2")
				return m
			}(), time.Now()),
			expected: false,
		},
		{
			name: "different last seen times",
			stamp1: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now().Add(-1*time.Hour)),
			stamp2: NewStamp("metric1", func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				return m
			}(), func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dpkey1", "dpvalue1")
				return m
			}(), time.Now()),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stamp1.Equals(tt.stamp2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkHashAttributes(b *testing.B) {
	attrs := pcommon.NewMap()
	attrs.PutStr("key1", "value1")
	attrs.PutStr("key2", "value2")
	attrs.PutStr("key3", "value3")

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hashAttributes(attrs)
	}
}

func BenchmarkHashMetricNameAndAttributes(b *testing.B) {
	attrs := pcommon.NewMap()
	attrs.PutStr("key1", "value1")
	attrs.PutStr("key2", "value2")
	dpattrs := pcommon.NewMap()
	dpattrs.PutStr("key3", "value3")

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hashMetric("metric1", attrs, dpattrs)
	}
}
