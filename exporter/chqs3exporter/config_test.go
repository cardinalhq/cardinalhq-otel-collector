// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.uber.org/multierr"
)

func TestLoadConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfig(filepath.Join("testdata", "default.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("chqs3")].(*Config)
	assert.Equal(t, e,
		&Config{
			IDSource: "env",
			S3Uploader: S3UploaderConfig{
				Region:      "us-east-1",
				S3Bucket:    "foo",
				S3Partition: "minute",
			},
			Timeboxes: TimeboxesConfig{
				Logs: TimeboxConfig{
					Interval:    60000,
					GracePeriod: 10000,
				},
				Metrics: TimeboxConfig{
					Interval:    10000,
					GracePeriod: 2000,
				},
				Traces: TimeboxConfig{
					Interval:    60000,
					GracePeriod: 10000,
				},
			},
			Buffering: BufferingConfig{
				Type: "memory",
			},
		},
	)
}

func TestConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("chqs3")].(*Config)
	expected := &Config{
		IDSource: "env",
		S3Uploader: S3UploaderConfig{
			Region:      "us-east-1",
			S3Bucket:    "foo",
			S3Prefix:    "bar",
			S3Partition: "minute",
			Endpoint:    "http://endpoint.com",
		},
		Buffering: BufferingConfig{
			Type: "memory",
		},
	}
	assert.Equal(t, expected, e)
}

func TestConfigForS3CompatibleSystems(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", "config-s3-compatible-systems.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("chqs3")].(*Config)
	expected := &Config{
		IDSource: "env",
		S3Uploader: S3UploaderConfig{
			Region:           "us-east-1",
			S3Bucket:         "foo",
			S3Prefix:         "bar",
			S3Partition:      "minute",
			Endpoint:         "alternative-s3-system.example.com",
			S3ForcePathStyle: true,
			DisableSSL:       true,
		},
		Buffering: BufferingConfig{
			Type: "memory",
		},
	}
	assert.Equal(t, expected, e)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		errExpected error
	}{
		{
			name: "valid",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "foo"
				c.S3Uploader.S3Bucket = "bar"
				c.S3Uploader.Endpoint = "http://example.com"
				return c
			}(),
			errExpected: nil,
		},
		{
			name: "missing all",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = ""
				return c
			}(),
			errExpected: multierr.Append(
				errors.New("region is required"),
				errors.New("bucket is required"),
			),
		},
		{
			name: "endpoint and region",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Endpoint = "http://example.com"
				c.S3Uploader.Region = "foo"
				return c
			}(),
			errExpected: errors.New("bucket is required"),
		},
		{
			name: "endpoint and bucket",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Endpoint = "http://example.com"
				c.S3Uploader.S3Bucket = "foo"
				c.S3Uploader.Region = ""
				return c
			}(),
			errExpected: errors.New("region is required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}

func TestTimeboxConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *TimeboxConfig
		errExpected error
	}{
		{
			name: "valid, disabled intervals",
			config: &TimeboxConfig{
				Interval:    0,
				GracePeriod: 0,
			},
			errExpected: nil,
		},
		{
			name: "valid, enabled, 0 grace",
			config: &TimeboxConfig{
				Interval:          1,
				GracePeriod:       0,
				OpenIntervalCount: 1,
			},
			errExpected: nil,
		},
		{
			name: "valid, enabled, non-zero grace",
			config: &TimeboxConfig{
				Interval:          1,
				GracePeriod:       1,
				OpenIntervalCount: 1,
			},
			errExpected: nil,
		},
		{
			name: "negative interval",
			config: &TimeboxConfig{
				Interval:          -1,
				GracePeriod:       0,
				OpenIntervalCount: 1,
			},
			errExpected: errors.New("interval must be greater than or equal to 0"),
		},
		{
			name: "enabled, missing grace period",
			config: &TimeboxConfig{
				Interval:          1,
				GracePeriod:       -1,
				OpenIntervalCount: 1,
			},
			errExpected: errors.New("grace period must be greater than or equal to 0"),
		},
		{
			name: "disabled, negative grace period",
			config: &TimeboxConfig{
				Interval:          0,
				GracePeriod:       -1,
				OpenIntervalCount: 1,
			},
			errExpected: nil,
		},
		{
			name: "enabled, missing open interval count",
			config: &TimeboxConfig{
				Interval:          1,
				GracePeriod:       0,
				OpenIntervalCount: 0,
			},
			errExpected: errors.New("open interval count must be greater than or equal to 1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}
