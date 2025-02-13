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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
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
					MetricName:          "metricName",
					ResourceAttributes:  pcommon.NewMap(),
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-time.Minute),
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
					MetricName:          "metricName1",
					ResourceAttributes:  pcommon.NewMap(),
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-time.Minute),
				},
				{
					MetricName:          "metricName2",
					ResourceAttributes:  pcommon.NewMap(),
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-2 * time.Minute),
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
					MetricName:          "metricName1",
					ResourceAttributes:  pcommon.NewMap(),
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-time.Minute),
				},
				{
					MetricName:          "metricName2",
					ResourceAttributes:  pcommon.NewMap(),
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-2 * time.Minute),
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
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-time.Minute),
				},
				{
					MetricName: "metricName2",
					ResourceAttributes: func() pcommon.Map {
						m := pcommon.NewMap()
						m.PutStr("key2", "value2")
						return m
					}(),
					DatapointAttributes: pcommon.NewMap(),
					LastSeen:            time.Now().Add(-2 * time.Minute),
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

func TestEmitList(t *testing.T) {
	tests := []struct {
		name          string
		config        *Config
		emitList      []Stamp
		consumer      *mockMetricsConsumer
		expectedCount int
	}{
		{
			name:          "Empty emit list",
			config:        &Config{NamePrefix: ""},
			emitList:      []Stamp{},
			consumer:      &mockMetricsConsumer{nil, nil},
			expectedCount: 0,
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
			consumer:      &mockMetricsConsumer{nil, nil},
			expectedCount: 1,
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
			consumer:      &mockMetricsConsumer{nil, nil},
			expectedCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{
				config:          tt.config,
				metricsConsumer: tt.consumer,
				logger:          zap.NewNop(),
			}

			md.emitList(tt.emitList)
			if tt.expectedCount == 0 {
				assert.Equal(t, 0, len(tt.consumer.received))
				return
			}
			require.Equal(t, 1, len(tt.consumer.received))
			assert.Equal(t, tt.expectedCount, tt.consumer.received[0].DataPointCount())
		})
	}
}

func TestConsumeMetrics(t *testing.T) {
	tests := []struct {
		name               string
		config             *Config
		inputMetrics       pmetric.Metrics
		expectedEntriesLen int
	}{
		{
			name: "Empty metrics",
			config: &Config{
				ResourceAttributesToCopy: []string{},
				metricAttributes: map[string][]string{
					"metricName": {"attr1", "attr2"},
				},
			},
			inputMetrics:       pmetric.NewMetrics(),
			expectedEntriesLen: 0,
		},
		{
			name: "Single metric",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				metricAttributes: map[string][]string{
					"metricName": {"attr1", "attr2"},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("key1", "value1")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("metricName")
				dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp.SetDoubleValue(1)
				return md
			}(),
			expectedEntriesLen: 1,
		},
		{
			name: "Multiple metrics with same resource",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				metricAttributes: map[string][]string{
					"metricName1": {"attr1", "attr2"},
					"metricName2": {"attr1", "attr2"},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("key1", "value1")
				sm := rm.ScopeMetrics().AppendEmpty()

				metric1 := sm.Metrics().AppendEmpty()
				metric1.SetName("metricName1")
				dp1 := metric1.SetEmptyGauge().DataPoints().AppendEmpty()
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetDoubleValue(1)

				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("metricName2")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntriesLen: 2,
		},
		{
			name: "Multiple metrics with different resources",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1", "key2"},
				metricAttributes: map[string][]string{
					"metricName1": {"attr1", "attr2"},
					"metricName2": {"attr1", "attr2"},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm1 := md.ResourceMetrics().AppendEmpty()
				rm1.Resource().Attributes().PutStr("key1", "value1")
				sm1 := rm1.ScopeMetrics().AppendEmpty()
				metric1 := sm1.Metrics().AppendEmpty()
				metric1.SetName("metricName1")
				dp1 := metric1.SetEmptyGauge().DataPoints().AppendEmpty()
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetDoubleValue(1)

				rm2 := md.ResourceMetrics().AppendEmpty()
				rm2.Resource().Attributes().PutStr("key2", "value2")
				sm2 := rm2.ScopeMetrics().AppendEmpty()
				metric2 := sm2.Metrics().AppendEmpty()
				metric2.SetName("metricName2")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntriesLen: 2,
		},
		{
			name: "Multiple metrics with the same name",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				metricAttributes: map[string][]string{
					"metricName": {"attr1", "attr2"},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("key1", "value1")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric1 := sm.Metrics().AppendEmpty()
				metric1.SetName("metricName")
				dp1 := metric1.SetEmptyGauge().DataPoints().AppendEmpty()
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetDoubleValue(1)

				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("metricName")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntriesLen: 1,
		},
		{
			name: "Multiple metrics with the same name and different resources",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				metricAttributes: map[string][]string{
					"metricName": {"attr1", "attr2"},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm1 := md.ResourceMetrics().AppendEmpty()
				rm1.Resource().Attributes().PutStr("key1", "value1")
				sm1 := rm1.ScopeMetrics().AppendEmpty()
				metric1 := sm1.Metrics().AppendEmpty()
				metric1.SetName("metricName")
				dp1 := metric1.SetEmptyGauge().DataPoints().AppendEmpty()
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetDoubleValue(1)

				rm2 := md.ResourceMetrics().AppendEmpty()
				rm2.Resource().Attributes().PutStr("key2", "value2")
				sm2 := rm2.ScopeMetrics().AppendEmpty()
				metric2 := sm2.Metrics().AppendEmpty()
				metric2.SetName("metricName")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntriesLen: 2,
		},
		{
			name: "Multiple metrics with the same name and the same resource but different attributes",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				metricAttributes: map[string][]string{
					"metricName": {"attr1", "attr2"},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("key1", "value1")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric1 := sm.Metrics().AppendEmpty()
				metric1.SetName("metricName")
				dp1 := metric1.SetEmptyGauge().DataPoints().AppendEmpty()
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp1.SetDoubleValue(1)
				dp1.Attributes().PutStr("attr1", "value1")

				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("metricName")
				metric2.SetDescription("description")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				dp2.SetDoubleValue(1)
				dp2.Attributes().PutStr("attr2", "value2")

				return md
			}(),
			expectedEntriesLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{
				config: tt.config,
			}

			err := md.ConsumeMetrics(context.Background(), tt.inputMetrics)
			require.NoError(t, err)

			count := 0
			md.entries.Range(func(key uint64, value *Stamp) bool {
				count++
				return true
			})
			assert.Equal(t, tt.expectedEntriesLen, count)
		})
	}
}

type mockMetricsConsumer struct {
	err      error
	received []pmetric.Metrics
}

func (m *mockMetricsConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	m.received = append(m.received, md)
	return m.err
}

func (m *mockMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
