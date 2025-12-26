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

package chqdatadogexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqdatadogexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfig(filepath.Join("testdata", "default.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("chqdatadog")].(*Config)
	expectedDefault := createDefaultConfig()
	c := expectedDefault.(*Config)
	c.APIKey = configopaque.String("1234567890abcdef1234567890abcdef")
	//	c.Metrics.APIKey = configopaque.String("1234567890abcdef1234567890abcdef")
	//	c.Logs.APIKey = configopaque.String("1234567890abcdef1234567890abcdef")
	//	c.Traces.APIKey = configopaque.String("1234567890abcdef1234567890abcdef")
	assert.Equal(t, expectedDefault, e)
}

func TestConfig(t *testing.T) {
	t.Parallel()

	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfig(filepath.Join("testdata", "config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("chqdatadog")].(*Config)
	// Note: With configopaque.MapList, user-provided headers in YAML replace defaults
	// (unlike the old map format which would merge). So User-Agent is not present
	// when custom headers are specified in the config file.
	expected := &Config{
		TimeoutConfig: exporterhelper.NewDefaultTimeoutConfig(),
		RetryConfig:   configretry.NewDefaultBackOffConfig(),
		QueueConfig:   configoptional.Optional[exporterhelper.QueueBatchConfig]{},
		APIKey:        configopaque.String("1234567890abcdef1234567890abcdef"),
		Metrics: MetricsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     500 * time.Millisecond,
				Endpoint:    "http://localhost:8080/metrics",
				Compression: configcompression.TypeGzip,
				Headers: configopaque.MapList{
					{
						Name:  "Alice",
						Value: "BobMetrics",
					},
				},
			},
		},
		Logs: LogsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     600 * time.Millisecond,
				Endpoint:    "http://localhost:8080/logs",
				Compression: configcompression.TypeZstd,
				Headers: configopaque.MapList{
					{
						Name:  "Alice",
						Value: "BobLogs",
					},
				},
			},
		},
		Traces: TracesConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     700 * time.Millisecond,
				Endpoint:    "http://localhost:8080/traces",
				Compression: configcompression.TypeDeflate,
				Headers: configopaque.MapList{
					{
						Name:  "Alice",
						Value: "BobTraces",
					},
				},
			},
		},
	}
	assert.Equal(t, expected, e)
}
