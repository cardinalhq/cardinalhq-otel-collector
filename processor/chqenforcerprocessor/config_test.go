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
	"go.uber.org/multierr"
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
		DropDecorationAttributes: defaultDropDecorationTags,
	}
	assert.Equal(t, expected, e)
}

// Test the Validate function for LogsConfig
func TestLogsConfigValidate(t *testing.T) {
	// Case 1: Valid LogsConfig with valid ContextID and Tags
	validConfig := &LogsConfig{
		StatsEnrichments: []StatsEnrichment{
			{
				Context: "log", // Valid context
				Tags:    []string{"tag1", "tag2"},
			},
			{
				Context: "resource", // Valid context
				Tags:    []string{"tag3"},
			},
		},
	}

	err := validConfig.Validate()
	assert.NoError(t, err, "Expected no error for valid config")

	// Case 2: Invalid ContextID
	invalidContextConfig := &LogsConfig{
		StatsEnrichments: []StatsEnrichment{
			{
				Context: "invalidContext", // Invalid context
				Tags:    []string{"tag1"},
			},
		},
	}

	err = invalidContextConfig.Validate()
	assert.Error(t, err, "Expected error for invalid context")
	assert.Contains(t, err.Error(), "invalid context: invalidContext", "Error message should mention invalid context")

	// Case 3: Empty Tags
	emptyTagsConfig := &LogsConfig{
		StatsEnrichments: []StatsEnrichment{
			{
				Context: "log",      // Valid context
				Tags:    []string{}, // Empty tags
			},
		},
	}

	err = emptyTagsConfig.Validate()
	assert.Error(t, err, "Expected error for empty tags")
	assert.Contains(t, err.Error(), "empty tags for context: log", "Error message should mention empty tags")

	// Case 4: Multiple Errors (Invalid ContextID and Empty Tags)
	multipleErrorsConfig := &LogsConfig{
		StatsEnrichments: []StatsEnrichment{
			{
				Context: "invalidContext", // Invalid context
				Tags:    []string{},       // Empty tags
			},
			{
				Context: "scope",    // Valid context
				Tags:    []string{}, // Empty tags
			},
		},
	}

	err = multipleErrorsConfig.Validate()
	assert.Error(t, err, "Expected multiple errors for invalid context and empty tags")

	// Check if both errors are captured
	errs := multierr.Errors(err)
	assert.Len(t, errs, 2, "Expected two errors: one for invalid context and one for empty tags")
	assert.Contains(t, errs[0].Error(), "invalid context: invalidContext", "First error should be about invalid context")
	assert.Contains(t, errs[1].Error(), "empty tags for context: scope", "Second error should be about empty tags")
}
