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

package chqenforcerprocessor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqenforcerprocessor/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Processors[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfig(filepath.Join("testdata", "default.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Processors[component.MustNewID("chqenforcer")].(*Config)
	assert.NoError(t, e.Validate())
	assert.NotNil(t, e.ConfigurationExtension)
	assert.NotNil(t, e.Statistics)
	e.ConfigurationExtension = nil
	assert.Equal(t, createDefaultConfig(), e)
}

func TestConfig(t *testing.T) {
	t.Parallel()

	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Processors[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfig(filepath.Join("testdata", "config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Processors[component.MustNewID("chqenforcer")].(*Config)
	assert.NoError(t, e.Validate())
	assert.NotNil(t, e.ConfigurationExtension)
	assert.NotNil(t, e.Statistics)
	e.ConfigurationExtension = nil
	expected := &Config{
		Statistics: StatisticsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     500 * time.Millisecond,
				Endpoint:    "http://localhost:8080",
				Compression: configcompression.TypeZstd,
				Headers: map[string]configopaque.String{
					"Alice":      "Bob",
					"User-Agent": "cardinalhq-otel-collector",
				},
			},
			Interval: 100 * time.Second,
			Phase:    "presample",
			Vendor:   "alice",
		},
		MetricAggregation: MetricAggregationConfig{
			Interval: 10 * time.Second,
		},
		TraceConfig: TraceConfig{
			GraphURL:            "http://example.com",
			UninterestingRate:   &defaultUninterestingRate,
			SlowRate:            &defaultSlowRate,
			HasErrorRate:        &defaultHasErrorRate,
			EstimatorWindowSize: &defaultEstimatorWindowSize,
			EstimatorInterval:   &defaultEstimatorInterval,
		},
		DropDecorationAttributes: defaultDropDecorationTags,
	}
	assert.Equal(t, expected, e)
}

func TestTraceConfig_Validate(t *testing.T) {
	//one := int(1)
	//zero := int(0)

	tests := []struct {
		name      string
		config    *TraceConfig
		wantError bool
	}{
		{
			"valid",
			&TraceConfig{
				GraphURL: "http://example.com",
			},
			false,
		},
		{
			"invalid URL",
			&TraceConfig{
				GraphURL: "://example.com",
			},
			true,
		},
		{
			"unsupported scheme",
			&TraceConfig{
				GraphURL: "ftp://example.com",
			},
			true,
		},
		{
			"no graphurl",
			&TraceConfig{},
			false,
		},
		{
			"invalid uninteresting rate",
			&TraceConfig{
				UninterestingRate: &[]int{-1}[0],
			},
			true,
		},
		{
			"invalid slow rate",
			&TraceConfig{
				SlowRate: &[]int{-1}[0],
			},
			true,
		},
		{
			"invalid has error rate",
			&TraceConfig{
				HasErrorRate: &[]int{-1}[0],
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.wantError, err != nil)
		})
	}
}

func TestTraceConfig_ValidateAppliesDefaults(t *testing.T) {
	cfg := &TraceConfig{}
	err := cfg.Validate()
	require.NoError(t, err)
	require.NotNil(t, cfg.UninterestingRate)
	require.NotNil(t, cfg.SlowRate)
	require.NotNil(t, cfg.HasErrorRate)
}
