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

package pitbullprocessor

import (
	"context"
	"testing"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

func TestConsumeMetrics(t *testing.T) {
	tests := []struct {
		name           string
		setupMetrics   func(t *testing.T) pmetric.Metrics
		setupProcessor func(t *testing.T) *pitbull
		expectedCheck  func(t *testing.T, md pmetric.Metrics, err error)
	}{
		{
			name: "NoResourceMetrics",
			setupMetrics: func(_ *testing.T) pmetric.Metrics {
				return pmetric.NewMetrics()
			},
			setupProcessor: func(_ *testing.T) *pitbull {
				return &pitbull{}
			},
			expectedCheck: func(t *testing.T, md pmetric.Metrics, err error) {
				assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
				assert.Equal(t, 0, md.DataPointCount())
			},
		},
		{
			name: "NoTransformationsOrLookupConfigs",
			setupMetrics: func(_ *testing.T) pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("org_id", "test_org")
				ilm := rm.ScopeMetrics().AppendEmpty()
				metric := ilm.Metrics().AppendEmpty()
				metric.SetName("api-gateway.movie_play_errors")
				g := metric.SetEmptyGauge()
				dp := g.DataPoints().AppendEmpty()
				dp.SetDoubleValue(1.23)
				dp.Attributes().PutStr("testattr", "testvalue")
				return md
			},
			setupProcessor: func(_ *testing.T) *pitbull {
				return &pitbull{}
			},
			expectedCheck: func(t *testing.T, md pmetric.Metrics, err error) {
				assert.NoError(t, err)
				assert.Equal(t, 1, md.DataPointCount())
			},
		},
		{
			name: "setsDropFlag",
			setupMetrics: func(_ *testing.T) pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				ilm := rm.ScopeMetrics().AppendEmpty()
				metric := ilm.Metrics().AppendEmpty()
				metric.SetName("api-gateway.movie_play_errors")
				g := metric.SetEmptyGauge()
				dp := g.DataPoints().AppendEmpty()
				dp.SetDoubleValue(1.23)
				dp.Attributes().PutStr("testattr", "testvalue")
				return md
			},
			setupProcessor: func(t *testing.T) *pitbull {
				p := &pitbull{
					logger:        zap.NewNop(),
					ttype:         "metrics",
					ottlTelemetry: &ottl.Telemetry{},
				}
				p.updateMetricConfigForTenant("default", &ottl.PitbullProcessorConfig{
					MetricStatements: makeStatements(),
				})
				_, found := p.metricTransformations.Load("default")
				require.True(t, found)
				return p
			},
			expectedCheck: func(t *testing.T, md pmetric.Metrics, err error) {
				assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
				assert.Equal(t, 0, md.DataPointCount())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := tt.setupMetrics(t)
			p := tt.setupProcessor(t)
			outmd, err := p.ConsumeMetrics(context.Background(), md)
			if tt.expectedCheck != nil {
				tt.expectedCheck(t, outmd, err)
			}
		})
	}
}

func makeStatements() []ottl.ContextStatement {
	statements := []ottl.ContextStatement{
		{
			Context: "datapoint",
			RuleId:  "drop_test",
			Statements: []string{
				`set(attributes["_cardinalhq.drop_marker"], true)`,
			},
			Conditions: []string{
				`String(metric.name) == "api-gateway.movie_play_errors"`,
			},
		},
		{
			Context: "datapoint",
			RuleId:  "no_conditions",
			Statements: []string{
				`set(attributes["foo"], true)`,
			},
			Conditions: []string{},
		},
	}
	return statements
}

func TestUpdateMetricConfigForTenant(t *testing.T) {
	tests := []struct {
		name           string
		cid            string
		pbc            *ottl.PitbullProcessorConfig
		setupProcessor func() *pitbull
		expectedCheck  func(t *testing.T, p *pitbull)
	}{
		{
			name: "NilConfig",
			cid:  "default",
			pbc:  nil,
			setupProcessor: func() *pitbull {
				p := &pitbull{
					logger: zap.NewNop(),
				}
				return p
			},
			expectedCheck: func(t *testing.T, p *pitbull) {
				_, found := p.metricTransformations.Load("default")
				assert.False(t, found)
			},
		},
		{
			name: "ValidConfig",
			cid:  "default",
			pbc: &ottl.PitbullProcessorConfig{
				MetricStatements: makeStatements(),
			},
			setupProcessor: func() *pitbull {
				p := &pitbull{
					logger: zap.NewNop(),
				}
				return p
			},
			expectedCheck: func(t *testing.T, p *pitbull) {
				xforms, found := p.metricTransformations.Load("default")
				assert.True(t, found)
				assert.NotNil(t, xforms)
			},
		},
		{
			name: "ErrorParsingTransformations",
			cid:  "default",
			pbc: &ottl.PitbullProcessorConfig{
				MetricStatements: []ottl.ContextStatement{
					{
						Context: "datapoint",
						RuleId:  "error in conditions",
						Statements: []string{
							`set(attributes["foo"], true)`,
						},
						Conditions: []string{
							"XXX",
						},
					},
				},
			},
			setupProcessor: func() *pitbull {
				p := &pitbull{
					logger: zap.NewNop(),
				}
				return p
			},
			expectedCheck: func(t *testing.T, p *pitbull) {
				_, found := p.metricTransformations.Load("default")
				assert.False(t, found)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.setupProcessor()
			p.updateMetricConfigForTenant(tt.cid, tt.pbc)
			// Add assertions here to verify the expected behavior
		})
	}
}

func BenchmarkConsumeMetrics(b *testing.B) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ilm := rm.ScopeMetrics().AppendEmpty()
	metric := ilm.Metrics().AppendEmpty()
	metric.SetName("api-gateway.movie_play_errors")
	g := metric.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.23)
	dp.Attributes().PutStr("testattr", "testvalue")

	p := &pitbull{
		logger:        zap.NewNop(),
		ttype:         "metrics",
		ottlTelemetry: &ottl.Telemetry{},
	}
	p.updateMetricConfigForTenant("default", &ottl.PitbullProcessorConfig{
		MetricStatements: makeStatements(),
	})
	_, found := p.metricTransformations.Load("default")
	require.True(b, found)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outmd, err := p.ConsumeMetrics(context.Background(), md)
		assert.ErrorIs(b, err, processorhelper.ErrSkipProcessingData)
		assert.Equal(b, 0, outmd.DataPointCount())
	}
}
