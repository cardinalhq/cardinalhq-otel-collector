// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.uber.org/multierr"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/metadata"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/notify"
)

func TestLoadConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", "default.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	encoding := component.MustNewIDWithName("foo", "bar")

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		QueueSettings:         queueCfg,
		TimeoutSettings:       timeoutCfg,
		Encoding:              &encoding,
		EncodingFileExtension: "baz",
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Some(func() exporterhelper.QueueBatchConfig {
		queue := exporterhelper.NewDefaultQueueConfig()
		queue.NumConsumers = 23
		queue.QueueSize = 42
		return queue
	}())

	timeoutCfg := exporterhelper.TimeoutConfig{
		Timeout: 8,
	}

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:              "us-east-1",
			S3Bucket:            "foo",
			S3Prefix:            "bar",
			S3PartitionFormat:   "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			S3PartitionTimezone: "Europe/London",
			Endpoint:            "http://endpoint.com",
			StorageClass:        "STANDARD",
			RetryMode:           DefaultRetryMode,
			RetryMaxAttempts:    DefaultRetryMaxAttempts,
			RetryMaxBackoff:     DefaultRetryMaxBackoff,
			Notifications:       notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfigS3StorageClass(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_storage_class.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD_IA",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3ACL(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_acl.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			ACL:               "bucket-owner-read",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3ACLDefined(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_canned-acl.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			ACL:               "bucket-owner-full-control",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigForS3CompatibleSystems(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3-compatible-systems.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "alternative-s3-system.example.com",
			S3ForcePathStyle:  true,
			DisableSSL:        true,
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		errExpected error
	}{
		{
			// endpoint overrides region and bucket name.
			name: "valid with endpoint and region",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Endpoint = "http://example.com"
				c.S3Uploader.Region = "foo"
				return c
			}(),
			errExpected: nil,
		},
		{
			// Endpoint will be built from bucket and region.
			// https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
			name: "valid with S3Bucket and region",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "foo"
				c.S3Uploader.S3Bucket = "bar"
				return c
			}(),
			errExpected: nil,
		},
		{
			name: "missing all",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = ""
				c.S3Uploader.S3Bucket = ""
				c.S3Uploader.Endpoint = ""
				return c
			}(),
			errExpected: multierr.Append(errors.New("region is required"),
				errors.New("bucket or endpoint is required")),
		},
		{
			name: "region only",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "foo"
				c.S3Uploader.S3Bucket = ""
				return c
			}(),
			errExpected: errors.New("bucket or endpoint is required"),
		},
		{
			name: "bucket only",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.S3Bucket = "foo"
				c.S3Uploader.Region = ""
				return c
			}(),
			errExpected: errors.New("region is required"),
		},
		{
			name: "endpoint only",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Endpoint = "http://example.com"
				c.S3Uploader.Region = ""
				return c
			}(),
			errExpected: errors.New("region is required"),
		},
		{
			name: "valid storage class GLACIER_IR",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "us-east-1"
				c.S3Uploader.S3Bucket = "mybucket"
				c.S3Uploader.StorageClass = "GLACIER_IR"
				return c
			}(),
			errExpected: nil,
		},
		{
			name: "valid storage class REDUCED_REDUNDANCY",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "us-east-1"
				c.S3Uploader.S3Bucket = "mybucket"
				c.S3Uploader.StorageClass = "REDUCED_REDUNDANCY"
				return c
			}(),
			errExpected: nil,
		},
		{
			name: "valid storage class EXPRESS_ONEZONE",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "us-east-1"
				c.S3Uploader.S3Bucket = "mybucket"
				c.S3Uploader.StorageClass = "EXPRESS_ONEZONE"
				return c
			}(),
			errExpected: nil,
		},
		{
			name: "invalid storage class FAKE_CLASS",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "us-east-1"
				c.S3Uploader.S3Bucket = "mybucket"
				c.S3Uploader.StorageClass = "FAKE_CLASS"
				return c
			}(),
			errExpected: errors.New("invalid StorageClass"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}

// TestConfigValidateNotifications covers the notifications block. Existing
// rows in TestConfig_Validate use require.Equal for exact error matching,
// which does not compose well with multierr-accumulated errors; a separate
// table keeps the assertions readable.
func TestConfigValidateNotifications(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*Config)
		wantErr     bool
		errContains string
	}{
		{
			name: "notifications disabled by default",
			mutate: func(*Config) {
				// createDefaultConfig populates Notifications with empty
				// Endpoint; Validate must accept it.
			},
			wantErr: false,
		},
		{
			name: "notifications enabled with minimal valid endpoint",
			mutate: func(c *Config) {
				c.S3Uploader.Notifications.Endpoint = "https://example.com/hook"
			},
			wantErr: false,
		},
		{
			name: "bad endpoint scheme",
			mutate: func(c *Config) {
				c.S3Uploader.Notifications.Endpoint = "ftp://example.com/hook"
			},
			wantErr:     true,
			errContains: "notifications.endpoint must be http(s) URL",
		},
		{
			name: "queue_size zero",
			mutate: func(c *Config) {
				c.S3Uploader.Notifications.Endpoint = "https://example.com/hook"
				c.S3Uploader.Notifications.QueueSize = 0
			},
			wantErr:     true,
			errContains: "queue_size must be >= 1",
		},
		{
			name: "max_backoff less than initial_backoff",
			mutate: func(c *Config) {
				c.S3Uploader.Notifications.Endpoint = "https://example.com/hook"
				c.S3Uploader.Notifications.InitialBackoff = 5 * time.Second
				c.S3Uploader.Notifications.MaxBackoff = 1 * time.Second
			},
			wantErr:     true,
			errContains: "max_backoff must be >= initial_backoff",
		},
		{
			name: "reserved Content-Type header",
			mutate: func(c *Config) {
				c.S3Uploader.Notifications.Endpoint = "https://example.com/hook"
				c.S3Uploader.Notifications.Headers.Set("Content-Type", "text/plain")
			},
			wantErr:     true,
			errContains: "Content-Type",
		},
		{
			name: "non-empty compression",
			mutate: func(c *Config) {
				c.S3Uploader.Notifications.Endpoint = "https://example.com/hook"
				c.S3Uploader.Notifications.Compression = "gzip"
			},
			wantErr:     true,
			errContains: "notifications.compression is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := createDefaultConfig().(*Config)
			c.S3Uploader.Region = "us-east-1"
			c.S3Uploader.S3Bucket = "bucket"
			tt.mutate(c)
			err := c.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMarshallerName(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "marshaler.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "sumo_ic",
	}, e,
	)

	e = cfg.Exporters[component.MustNewIDWithName("awss3", "proto")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_proto",
	}, e,
	)
}

func TestCompressionName(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "compression.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Compression:       "gzip",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
	}, e,
	)

	e = cfg.Exporters[component.MustNewIDWithName("awss3", "proto")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Compression:       "none",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_proto",
	}, e,
	)

	e = cfg.Exporters[component.MustNewIDWithName("awss3", "zstd")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Compression:       "zstd",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestResourceAttrsToS3(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_resource-attrs-to-s3.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
		ResourceAttrsToS3: ResourceAttrsToS3{
			S3Bucket: "com.awss3.bucket",
			S3Prefix: "com.awss3.prefix",
		},
	}, e,
	)
}

func TestRetry(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "retry.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD_IA",
			RetryMode:         "standard",
			RetryMaxAttempts:  5,
			RetryMaxBackoff:   30 * time.Second,
			Notifications:     notify.NewDefaultConfig(),
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfigS3UniqueKeyFunc(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_unique_key_func.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			StorageClass:      "STANDARD",
			UniqueKeyFuncName: "uuidv7",
			Notifications:     notify.NewDefaultConfig(),
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3BasePrefix(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_base_prefix.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.None[exporterhelper.QueueBatchConfig]()
	timeoutCfg := exporterhelper.TimeoutConfig{
		Timeout: 5 * time.Second,
	}

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3BasePrefix:      "base/prefix",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3BasePrefixWithResourceAttrs(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_base_prefix_with_resource_attrs.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.None[exporterhelper.QueueBatchConfig]()
	timeoutCfg := exporterhelper.TimeoutConfig{
		Timeout: 5 * time.Second,
	}

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "default-metric",
			S3BasePrefix:      "environment/prod",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			Notifications:     notify.NewDefaultConfig(),
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
		ResourceAttrsToS3: ResourceAttrsToS3{
			S3Prefix: "com.awss3.prefix",
		},
	}, e,
	)
}

// TestConfigNotifications loads a full notifications block from YAML and
// asserts that every field decodes to the right Go value. This is the
// canonical round-trip test for the mapstructure wiring.
func TestConfigNotifications(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	require.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-notifications.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	n := e.S3Uploader.Notifications

	assert.Equal(t, "https://example.com/webhook", n.Endpoint)
	assert.Equal(t, 7*time.Second, n.Timeout)
	assert.Equal(t, 1234, n.QueueSize)
	assert.Equal(t, 2, n.Workers)
	assert.Equal(t, 50, n.MaxRecordsPerPost)
	assert.Equal(t, 5, n.MaxAttempts)
	assert.Equal(t, 2*time.Second, n.InitialBackoff)
	assert.Equal(t, 40*time.Second, n.MaxBackoff)

	v, ok := n.Headers.Get("Authorization")
	require.True(t, ok, "Authorization header must round-trip through config")
	assert.Equal(t, "Bearer abc", string(v))
}
