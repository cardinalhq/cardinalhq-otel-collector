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
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/converter/expandconverter"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqdatadogexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfigWithSettings(factories, otelcol.ConfigProviderSettings{
		ResolverSettings: confmap.ResolverSettings{
			URIs: []string{filepath.Join("testdata", "default.yaml")},
			ProviderFactories: []confmap.ProviderFactory{
				fileprovider.NewFactory(),
				envprovider.NewFactory(),
				yamlprovider.NewFactory(),
				httpprovider.NewFactory(),
			},
			ConverterFactories: []confmap.ConverterFactory{expandconverter.NewFactory()},
		},
	})
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

	cfg, err := otelcoltest.LoadConfigWithSettings(factories, otelcol.ConfigProviderSettings{
		ResolverSettings: confmap.ResolverSettings{
			URIs: []string{filepath.Join("testdata", "config.yaml")},
			ProviderFactories: []confmap.ProviderFactory{
				fileprovider.NewFactory(),
				envprovider.NewFactory(),
				yamlprovider.NewFactory(),
				httpprovider.NewFactory(),
			},
			ConverterFactories: []confmap.ConverterFactory{expandconverter.NewFactory()},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("chqdatadog")].(*Config)
	expected := &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
		RetryConfig:     configretry.NewDefaultBackOffConfig(),
		QueueConfig:     exporterhelper.NewDefaultQueueSettings(),
		APIKey:          configopaque.String("1234567890abcdef1234567890abcdef"),
		Metrics: MetricsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     500 * time.Millisecond,
				Endpoint:    "http://localhost:8080/metrics",
				Compression: configcompression.TypeGzip,
				Headers: map[string]configopaque.String{
					"Alice":      "BobMetrics",
					"User-Agent": "cardinalhq-otel-collector-chqdatadogexporter",
				},
			},
		},
		Logs: LogsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     600 * time.Millisecond,
				Endpoint:    "http://localhost:8080/logs",
				Compression: configcompression.TypeZstd,
				Headers: map[string]configopaque.String{
					"Alice":      "BobLogs",
					"User-Agent": "cardinalhq-otel-collector-chqdatadogexporter",
				},
			},
		},
		Traces: TracesConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:     700 * time.Millisecond,
				Endpoint:    "http://localhost:8080/traces",
				Compression: configcompression.TypeDeflate,
				Headers: map[string]configopaque.String{
					"Alice":      "BobTraces",
					"User-Agent": "cardinalhq-otel-collector-chqdatadogexporter",
				},
			},
		},
	}
	assert.Equal(t, expected, e)
}
