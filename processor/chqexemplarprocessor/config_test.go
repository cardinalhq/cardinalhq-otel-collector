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

package chqexemplarprocessor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqexemplarprocessor/internal/metadata"
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

	e := cfg.Processors[component.MustNewID(metadata.Type.String())].(*Config)
	assert.NoError(t, e.Validate())
	assert.NotNil(t, e.Reporting)
	expected := createDefaultConfig()
	actualConf := expected.(*Config)
	actualConf.Endpoint = "http://example.com"
	assert.Equal(t, actualConf, e)
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

	e := cfg.Processors[component.MustNewID(metadata.Type.String())].(*Config)
	assert.NoError(t, e.Validate())
	assert.NotNil(t, e.Reporting)
	expected := &Config{
		ClientConfig: confighttp.ClientConfig{
			Endpoint: "http://example.com",
		},
		Reporting: ReportingConfig{
			Logs: EnabledOption{
				Enabled:   true,
				Expiry:    10 * time.Minute,
				Interval:  5 * time.Minute,
				CacheSize: 1001,
				BatchSize: 200,
			},
			Metrics: EnabledOption{
				Enabled:   true,
				Expiry:    11 * time.Minute,
				Interval:  4 * time.Minute,
				CacheSize: 1002,
				BatchSize: 200,
			},
			Traces: EnabledOption{
				Enabled:   true,
				Expiry:    12 * time.Minute,
				Interval:  3 * time.Minute,
				CacheSize: 1003,
				BatchSize: 200,
			},
		},
	}
	assert.Equal(t, expected, e)
}
