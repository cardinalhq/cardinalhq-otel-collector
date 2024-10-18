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

package chqstatsprocessor

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

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqstatsprocessor/internal/metadata"
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

	e := cfg.Processors[component.MustNewID("chqstats")].(*Config)
	assert.NoError(t, e.Validate())
	assert.NotNil(t, e.Statistics)
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

	e := cfg.Processors[component.MustNewID("chqstats")].(*Config)
	assert.NoError(t, e.Validate())
	assert.NotNil(t, e.Statistics)
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
	}
	assert.Equal(t, expected, e)
}
