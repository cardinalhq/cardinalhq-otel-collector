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

	"github.com/cardinalhq/cardinalhq-otel-collector/connector/chqmissingdataconnector/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestBuildMetrics(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		config   *Config
		emitList []*stamp
		verify   func(t *testing.T, metrics pmetric.Metrics)
	}{
		{
			name:     "Empty emit list",
			config:   getDefaultConfig(),
			emitList: []*stamp{},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 0, metrics.DataPointCount())
			},
		},
		{
			name:   "Single stamp",
			config: getDefaultConfig(),
			emitList: []*stamp{
				newStamp("metricName", pcommon.NewMap(), pcommon.NewMap(), now.Add(-time.Minute)),
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 1, metrics.DataPointCount())
				rm := metrics.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				metric := sm.Metrics().At(0)

				assert.Equal(t, defaultMetricName, metric.Name())
				assert.Equal(t, "Missing data age in seconds", metric.Description())
				assert.Equal(t, "s", metric.Unit())

				assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())

				dp := metric.Gauge().DataPoints().At(0)
				assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, dp.ValueType())
				v, found := dp.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName", v.AsString())
			},
		},
		{
			name:   "Multiple stamps",
			config: getDefaultConfig(),
			emitList: []*stamp{
				newStamp("metricName1", pcommon.NewMap(), pcommon.NewMap(), now.Add(-time.Minute)),
				newStamp("metricName2", pcommon.NewMap(), pcommon.NewMap(), now.Add(-2*time.Minute)),
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 2, metrics.DataPointCount())
				rm := metrics.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				metric1 := sm.Metrics().At(0)
				metric2 := sm.Metrics().At(1)
				assert.Equal(t, defaultMetricName, metric1.Name())
				assert.Equal(t, defaultMetricName, metric2.Name())

				assert.Equal(t, pmetric.MetricTypeGauge, metric1.Type())
				assert.Equal(t, pmetric.MetricTypeGauge, metric2.Type())

				dp1 := metric1.Gauge().DataPoints().At(0)
				dp2 := metric2.Gauge().DataPoints().At(0)

				v, found := dp1.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName1", v.AsString())

				v, found = dp2.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName2", v.AsString())
			},
		},
		{
			name:   "Multiple stamps with same resource",
			config: getDefaultConfig(),
			emitList: []*stamp{
				newStamp("metricName1", pcommon.NewMap(), pcommon.NewMap(), now.Add(-time.Minute)),
				newStamp("metricName2", pcommon.NewMap(), pcommon.NewMap(), now.Add(-2*time.Minute)),
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 2, metrics.DataPointCount())
				rm := metrics.ResourceMetrics().At(0)
				sm := rm.ScopeMetrics().At(0)
				metric1 := sm.Metrics().At(0)
				metric2 := sm.Metrics().At(1)
				assert.Equal(t, defaultMetricName, metric1.Name())
				assert.Equal(t, defaultMetricName, metric2.Name())

				assert.Equal(t, pmetric.MetricTypeGauge, metric1.Type())
				assert.Equal(t, pmetric.MetricTypeGauge, metric2.Type())

				dp1 := metric1.Gauge().DataPoints().At(0)
				dp2 := metric2.Gauge().DataPoints().At(0)

				v, found := dp1.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName1", v.AsString())

				v, found = dp2.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName2", v.AsString())
			},
		},
		{
			name:   "Multiple stamps with different resources",
			config: getDefaultConfig(),
			emitList: []*stamp{
				newStamp("metricName1", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), pcommon.NewMap(), now.Add(-time.Minute)),
				newStamp("metricName2", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key2", "value2")
					return m
				}(), pcommon.NewMap(), now.Add(-2*time.Minute)),
			},
			verify: func(t *testing.T, metrics pmetric.Metrics) {
				assert.Equal(t, 2, metrics.DataPointCount())
				rm1 := metrics.ResourceMetrics().At(0)
				rm2 := metrics.ResourceMetrics().At(1)
				sm1 := rm1.ScopeMetrics().At(0)
				sm2 := rm2.ScopeMetrics().At(0)
				metric1 := sm1.Metrics().At(0)
				metric2 := sm2.Metrics().At(0)
				assert.Equal(t, defaultMetricName, metric1.Name())
				assert.Equal(t, defaultMetricName, metric2.Name())

				v, found := rm1.Resource().Attributes().Get("key1")
				assert.True(t, found)
				assert.Equal(t, "value1", v.AsString())

				v, found = rm2.Resource().Attributes().Get("key2")
				assert.True(t, found)
				assert.Equal(t, "value2", v.AsString())

				assert.Equal(t, pmetric.MetricTypeGauge, metric1.Type())
				assert.Equal(t, pmetric.MetricTypeGauge, metric2.Type())

				dp1 := metric1.Gauge().DataPoints().At(0)
				dp2 := metric2.Gauge().DataPoints().At(0)

				v, found = dp1.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName1", v.AsString())

				v, found = dp2.Attributes().Get(defaultMetricNameAttribute)
				assert.True(t, found)
				assert.Equal(t, "metricName2", v.AsString())

				assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, dp1.ValueType())
				assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, dp2.ValueType())
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
	now := time.Now()
	tests := []struct {
		name          string
		config        *Config
		emitList      []*stamp
		consumer      *mockMetricsConsumer
		expectedCount int
	}{
		{
			name:          "Empty emit list",
			config:        getDefaultConfig(),
			emitList:      []*stamp{},
			consumer:      &mockMetricsConsumer{nil, nil},
			expectedCount: 0,
		},
		{
			name:   "Single stamp",
			config: getDefaultConfig(),
			emitList: []*stamp{
				newStamp("metricName", pcommon.NewMap(), pcommon.NewMap(), now.Add(-time.Minute)),
			},
			consumer:      &mockMetricsConsumer{nil, nil},
			expectedCount: 1,
		},
		{
			name:   "Multiple stamps",
			config: getDefaultConfig(),
			emitList: []*stamp{
				newStamp("metricName1", pcommon.NewMap(), pcommon.NewMap(), now.Add(-time.Minute)),
				newStamp("metricName2", pcommon.NewMap(), pcommon.NewMap(), now.Add(-2*time.Minute)),
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
	now := time.Now()
	tests := []struct {
		name            string
		config          *Config
		inputMetrics    pmetric.Metrics
		expectedEntries []*stamp
	}{
		{
			name: "Empty metrics",
			config: &Config{
				ResourceAttributesToCopy: []string{},
				Metrics: []MetricConfig{
					{
						Name:       "metricName",
						Attributes: []string{"attr1", "attr2"},
					},
				},
			},
			inputMetrics:    pmetric.NewMetrics(),
			expectedEntries: []*stamp{},
		},
		{
			name: "Single metric",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				Metrics: []MetricConfig{
					{
						Name:               "metricName",
						Attributes:         []string{"attr1", "attr2"},
						ResourceAttributes: []string{"resourceKey1", "resourceKey2"},
					},
				},
			},
			inputMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("key1", "value1")
				rm.Resource().Attributes().PutStr("resourceKey1", "resourceValue1")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("metricName")
				dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp.SetDoubleValue(1)
				return md
			}(),
			expectedEntries: []*stamp{
				newStamp("metricName", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					m.PutStr("resourceKey1", "resourceValue1")
					return m
				}(), pcommon.NewMap(), now),
			},
		},
		{
			name: "Multiple metrics with same resource",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				Metrics: []MetricConfig{
					{
						Name:       "metricName1",
						Attributes: []string{"attr1", "attr2"},
					},
					{
						Name:       "metricName2",
						Attributes: []string{"attr1", "attr2"},
					},
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
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetDoubleValue(1)

				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("metricName2")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntries: []*stamp{
				newStamp("metricName1", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), pcommon.NewMap(), now),
				newStamp("metricName2", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), pcommon.NewMap(), now),
			},
		},
		{
			name: "Multiple metrics with different resources",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1", "key2"},
				Metrics: []MetricConfig{
					{
						Name:       "metricName1",
						Attributes: []string{"attr1", "attr2"},
					},
					{
						Name:       "metricName2",
						Attributes: []string{"attr1", "attr2"},
					},
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
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetDoubleValue(1)

				rm2 := md.ResourceMetrics().AppendEmpty()
				rm2.Resource().Attributes().PutStr("key2", "value2")
				sm2 := rm2.ScopeMetrics().AppendEmpty()
				metric2 := sm2.Metrics().AppendEmpty()
				metric2.SetName("metricName2")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntries: []*stamp{
				newStamp("metricName1", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), pcommon.NewMap(), now),
				newStamp("metricName2", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key2", "value2")
					return m
				}(), pcommon.NewMap(), now),
			},
		},
		{
			name: "Multiple metrics with the same name",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				Metrics: []MetricConfig{
					{
						Name:       "metricName",
						Attributes: []string{"attr1", "attr2"},
					},
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
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetDoubleValue(1)

				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("metricName")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntries: []*stamp{
				newStamp("metricName", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), pcommon.NewMap(), now),
			},
		},
		{
			name: "Multiple metrics with the same name and different resources",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				Metrics: []MetricConfig{
					{
						Name:       "metricName",
						Attributes: []string{"attr1", "attr2"},
					},
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
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetDoubleValue(1)

				rm2 := md.ResourceMetrics().AppendEmpty()
				rm2.Resource().Attributes().PutStr("key2", "value2")
				sm2 := rm2.ScopeMetrics().AppendEmpty()
				metric2 := sm2.Metrics().AppendEmpty()
				metric2.SetName("metricName")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetDoubleValue(1)

				return md
			}(),
			expectedEntries: []*stamp{
				newStamp("metricName", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), pcommon.NewMap(), now),
				newStamp("metricName", pcommon.NewMap(), pcommon.NewMap(), now),
			},
		},
		{
			name: "Multiple metrics with the same name and the same resource but different attributes",
			config: &Config{
				ResourceAttributesToCopy: []string{"key1"},
				Metrics: []MetricConfig{
					{
						Name:       "metricName",
						Attributes: []string{"attr1", "attr2"},
					},
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
				dp1.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp1.SetDoubleValue(1)
				dp1.Attributes().PutStr("attr1", "value1")

				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("metricName")
				metric2.SetDescription("description")
				dp2 := metric2.SetEmptyGauge().DataPoints().AppendEmpty()
				dp2.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
				dp2.SetDoubleValue(1)
				dp2.Attributes().PutStr("attr2", "value2")

				return md
			}(),
			expectedEntries: []*stamp{
				newStamp("metricName", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("attr1", "value1")
					return m
				}(), now),
				newStamp("metricName", func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("key1", "value1")
					return m
				}(), func() pcommon.Map {
					m := pcommon.NewMap()
					m.PutStr("attr2", "value2")
					return m
				}(), now),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{
				config: tt.config,
			}
			md.setupStaticConfig()

			err := md.ConsumeMetrics(context.Background(), tt.inputMetrics)
			require.NoError(t, err)

			values := []*stamp{}
			md.entries.Range(func(_ uint64, value *stamp) bool {
				values = append(values, value)
				return true
			})
			assertStampsEqual(t, tt.expectedEntries, values)
		})
	}
}

func TestFilteredAttributes(t *testing.T) {
	tests := []struct {
		name     string
		attrs    pcommon.Map
		keys     []string
		expected pcommon.Map
	}{
		{
			name: "No attributes",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				return m
			}(),
			keys: []string{"key1", "key2"},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				return m
			}(),
		},
		{
			name: "No keys",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutInt("key2", 2)
				return m
			}(),
			keys: []string{},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				return m
			}(),
		},
		{
			name: "Some matching keys",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutInt("key2", 2)
				m.PutStr("key3", "value3")
				return m
			}(),
			keys: []string{"key1", "key3"},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutStr("key3", "value3")
				return m
			}(),
		},
		{
			name: "All matching keys",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutInt("key2", 2)
				return m
			}(),
			keys: []string{"key1", "key2"},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutInt("key2", 2)
				return m
			}(),
		},
		{
			name: "No matching keys",
			attrs: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key1", "value1")
				m.PutStr("key2", "value2")
				return m
			}(),
			keys: []string{"key3", "key4"},
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				return m
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filteredAttributes(tt.attrs, tt.keys)
			assert.Equal(t, tt.expected.AsRaw(), result.AsRaw())
		})
	}
}

func TestBuildEmitList(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name          string
		config        *Config
		entries       map[uint64]*stamp
		now           time.Time
		expectedEmit  []*stamp
		expectedCount int
	}{
		{
			name: "No entries",
			config: &Config{
				MaximumAge: time.Minute,
			},
			entries:       map[uint64]*stamp{},
			now:           now,
			expectedEmit:  []*stamp{},
			expectedCount: 0,
		},
		{
			name: "Single non-expired entry",
			config: &Config{
				MaximumAge: time.Minute,
			},
			entries: map[uint64]*stamp{
				1: newStamp("metricName", pcommon.NewMap(), pcommon.NewMap(), now.Add(-30*time.Second)),
			},
			now: now,
			expectedEmit: []*stamp{
				newStamp("metricName", pcommon.NewMap(), pcommon.NewMap(), now.Add(-30*time.Second)),
			},
			expectedCount: 1,
		},
		{
			name: "Single expired entry",
			config: &Config{
				MaximumAge: time.Minute,
			},
			entries: map[uint64]*stamp{
				1: newStamp("metricName", pcommon.NewMap(), pcommon.NewMap(), now.Add(-2*time.Minute)),
			},
			now:           now,
			expectedEmit:  []*stamp{},
			expectedCount: 0,
		},
		{
			name: "Multiple entries with mixed expiration",
			config: &Config{
				MaximumAge: time.Minute,
			},
			entries: map[uint64]*stamp{
				1: newStamp("metricName1", pcommon.NewMap(), pcommon.NewMap(), now.Add(-30*time.Second)),
				2: newStamp("metricName2", pcommon.NewMap(), pcommon.NewMap(), now.Add(-2*time.Minute)),
			},
			now: now,
			expectedEmit: []*stamp{
				newStamp("metricName1", pcommon.NewMap(), pcommon.NewMap(), now.Add(-30*time.Second)),
			},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{
				config: tt.config,
			}

			for k, v := range tt.entries {
				md.entries.Store(k, v)
			}

			emitList := md.buildEmitList(tt.now)
			assert.Equal(t, tt.expectedCount, len(emitList))
			assertStampsEqual(t, tt.expectedEmit, emitList)
		})
	}
}

func TestConfigUpdateCallback(t *testing.T) {
	tests := []struct {
		name            string
		initialTenants  map[string]*tenantConfig
		config          ottl.ControlPlaneConfig
		expectedTenants map[string]*tenantConfig
	}{
		{
			name: "Empty config",
			initialTenants: map[string]*tenantConfig{
				"tenant1": {},
			},
			config: ottl.ControlPlaneConfig{
				Configs: map[string]ottl.TenantConfig{},
			},
			expectedTenants: map[string]*tenantConfig{},
		},
		{
			name:           "Add new tenant",
			initialTenants: map[string]*tenantConfig{},
			config: ottl.ControlPlaneConfig{
				Configs: map[string]ottl.TenantConfig{
					"tenant1": {
						MissingDataConfig: map[string]*ottl.MissingDataConfig{
							"connectorName": {
								Metrics: []ottl.MissingDataMetric{
									{Name: "metric1"},
								},
							},
						},
					},
				},
			},
			expectedTenants: map[string]*tenantConfig{
				"tenant1": {
					metricAttributes: map[string][]string{
						"metric1": nil,
					},
					resourceAttributes: map[string][]string{
						"metric1": nil,
					},
				},
			},
		},
		{
			name: "Remove tenant",
			initialTenants: map[string]*tenantConfig{
				"tenant1": {},
			},
			config: ottl.ControlPlaneConfig{
				Configs: map[string]ottl.TenantConfig{},
			},
			expectedTenants: map[string]*tenantConfig{},
		},
		{
			name: "Update tenant",
			initialTenants: map[string]*tenantConfig{
				"tenant1": {
					metricAttributes: map[string][]string{
						"metric1": nil,
					},
					resourceAttributes: map[string][]string{
						"metric1": nil,
					},
				},
			},
			config: ottl.ControlPlaneConfig{
				Configs: map[string]ottl.TenantConfig{
					"tenant1": {
						MissingDataConfig: map[string]*ottl.MissingDataConfig{
							"connectorName": {
								Metrics: []ottl.MissingDataMetric{
									{Name: "metric2"},
								},
							},
						},
					},
				},
			},
			expectedTenants: map[string]*tenantConfig{
				"tenant1": {
					metricAttributes: map[string][]string{
						"metric2": nil,
					},
					resourceAttributes: map[string][]string{
						"metric2": nil,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{
				id: component.NewIDWithName(metadata.Type, "connectorName"),
			}

			for k, v := range tt.initialTenants {
				md.tenants.Store(k, v)
			}

			md.configUpdateCallback(tt.config)

			actualTenants := map[string]*tenantConfig{}
			md.tenants.Range(func(key string, value *tenantConfig) bool {
				actualTenants[key] = value
				return true
			})

			assert.Equal(t, tt.expectedTenants, actualTenants)
		})
	}
}

func TestBuildAttributeMaps(t *testing.T) {
	tests := []struct {
		name          string
		tid           string
		initialTenant *tenantConfig
		metrics       []ottl.MissingDataMetric
		expected      *tenantConfig
	}{
		{
			name:          "Empty metrics",
			tid:           "tenant1",
			initialTenant: &tenantConfig{},
			metrics:       []ottl.MissingDataMetric{},
			expected: &tenantConfig{
				metricAttributes:   map[string][]string{},
				resourceAttributes: map[string][]string{},
			},
		},
		{
			name:          "Single metric",
			tid:           "tenant1",
			initialTenant: &tenantConfig{},
			metrics: []ottl.MissingDataMetric{
				{
					Name:               "metric1",
					Attributes:         []string{"attr1", "attr2"},
					ResourceAttributes: []string{"resAttr1", "resAttr2"},
				},
			},
			expected: &tenantConfig{
				metricAttributes: map[string][]string{
					"metric1": {"attr1", "attr2"},
				},
				resourceAttributes: map[string][]string{
					"metric1": {"resAttr1", "resAttr2"},
				},
			},
		},
		{
			name:          "Multiple metrics",
			tid:           "tenant1",
			initialTenant: &tenantConfig{},
			metrics: []ottl.MissingDataMetric{
				{
					Name:               "metric1",
					Attributes:         []string{"attr1", "attr2"},
					ResourceAttributes: []string{"resAttr1", "resAttr2"},
				},
				{
					Name:               "metric2",
					Attributes:         []string{"attr3", "attr4"},
					ResourceAttributes: []string{"resAttr3", "resAttr4"},
				},
			},
			expected: &tenantConfig{
				metricAttributes: map[string][]string{
					"metric1": {"attr1", "attr2"},
					"metric2": {"attr3", "attr4"},
				},
				resourceAttributes: map[string][]string{
					"metric1": {"resAttr1", "resAttr2"},
					"metric2": {"resAttr3", "resAttr4"},
				},
			},
		},
		{
			name: "update tenant",
			tid:  "tenant1",
			initialTenant: &tenantConfig{
				metricAttributes: map[string][]string{
					"metric1": {"attr1", "attr2"},
				},
				resourceAttributes: map[string][]string{
					"metric1": {"resAttr1", "resAttr2"},
				},
			},
			metrics: []ottl.MissingDataMetric{
				{
					Name:               "metric2",
					Attributes:         []string{"attr3", "attr4"},
					ResourceAttributes: []string{"resAttr3", "resAttr4"},
				},
			},
			expected: &tenantConfig{
				metricAttributes: map[string][]string{
					"metric2": {"attr3", "attr4"},
				},
				resourceAttributes: map[string][]string{
					"metric2": {"resAttr3", "resAttr4"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &md{}
			md.buildAttributeMaps(tt.tid, tt.metrics)

			actual, ok := md.tenants.Load(tt.tid)
			require.True(t, ok)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func assertStampsEqual(t *testing.T, expected []*stamp, actual []*stamp) {
	expectedStrings := []string{}
	for _, s := range expected {
		expectedStrings = append(expectedStrings, s.String())
	}

	actualStrings := []string{}
	for _, s := range actual {
		actualStrings = append(actualStrings, s.String())
	}

	assert.ElementsMatch(t, expectedStrings, actualStrings)
}

type mockMetricsConsumer struct {
	err      error
	received []pmetric.Metrics
}

func (m *mockMetricsConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	m.received = append(m.received, md)
	return m.err
}

func (m *mockMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
