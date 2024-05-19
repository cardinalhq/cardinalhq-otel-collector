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

package chqdecoratorprocessor

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/filereader"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/s3tools"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/sampler"
	"github.com/hashicorp/go-multierror"
)

type Config struct {
	// SamplerConfigFile is the URL of the file containing the configuration for the sampler.
	SamplerConfigFile string `mapstructure:"sampler_config_file"`

	// S3 configuration
	Region     string `mapstructure:"region"`
	S3Endpoint string `mapstructure:"s3_endpoint"`
	S3Provider string `mapstructure:"s3_provider"`

	// ConfigCheckInterval is the interval at which the configuration file is checked for updates.
	ConfigCheckInterval int `mapstructure:"config_check_interval"`

	// ClusterID is the ID of the cluster to which the collector belongs.
	// All collectors able to receive the same data should use this same value.
	ClusterID string `mapstructure:"cluster_id"`

	// APIKey is the API key to use when fetching configuration via http[s].
	// If it is not set, no authorization header is added.
	APIKey string `mapstructure:"api_key"`

	MetricConfig        MetricConfig `mapstructure:"metrics"`
	configCheckInterval time.Duration

	TraceConfig TraceConfig `mapstructure:"traces"`
}

// MetricConfig contains configuration for the metrics processor.
type MetricConfig struct {
	// MetricAggregationInterval is the interval at which metrics are aggregated.
	// Metrics are gathered over time, and sent to the next processor in
	// our pipeline in blocks of this duration.
	// An interval is considered "closed" when it is at least
	// one interval in the past.
	MetricAggregationInterval int64 `mapstructure:"metric_aggregation_interval"`
}

type TraceConfig struct {
	// Where to send the graph data.  This will be done using a HTTP post.
	GraphURL string `mapstructure:"graph_url"`
	// UninterestingRate is the rate limit applied to traces that
	// are not otherwise interesting.  Defaults to 0, which will
	// drop all uninteresting traces.  A value of 100 will keep
	// 1 out of 100 traces.
	// The rate is applied per fingerprint.
	UninterestingRate *int `mapstructure:"uninteresting_rate"`
	// SlowRate is the rate limit applied to traces that are considered
	// slow by measuring the duration of this trace and comparing it to
	// the 75% percentile of previous traces that are the same shape,
	// i.e., have the same fingerprint.
	// The rate is applied per fingerprint.
	SlowRate *int `mapstructure:"slow_rate"`
	// HasErrorRate is the rate limit applied to traces that have
	// at least one span that has an error indication.
	// The rate is applied per fingerprint.
	HasErrorRate *int `mapstructure:"has_error_rate"`

	EstimatorWindowSize *int   `mapstructure:"estimator_window_size"`
	EstimatorInterval   *int64 `mapstructure:"estimator_interval"`
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {
	if cfg.configCheckInterval <= 0 {
		cfg.configCheckInterval = 10 * time.Second
	}
	cfg.configCheckInterval = time.Duration(cfg.ConfigCheckInterval) * time.Second

	if cfg.MetricConfig.MetricAggregationInterval < 10 {
		return fmt.Errorf("invalid metric aggregation interval: %d, must be >= than 10", cfg.MetricConfig.MetricAggregationInterval)
	}

	if cfg.SamplerConfigFile != "" {
		return checkSamplerConfigFile(cfg.SamplerConfigFile)
	}

	return cfg.TraceConfig.Validate()
}

func (cfg *TraceConfig) Validate() error {
	var errors *multierror.Error

	if cfg.GraphURL != "" {
		u, err := url.Parse(cfg.GraphURL)
		if err != nil {
			err := fmt.Errorf("invalid URL: %w", err)
			errors = multierror.Append(errors, err)
		} else {
			if u.Scheme != "http" && u.Scheme != "https" {
				err := fmt.Errorf("unsupported scheme: %s, must be 'http' or 'https'", u.Scheme)
				errors = multierror.Append(errors, err)
			}
		}
	}

	if cfg.UninterestingRate == nil {
		cfg.UninterestingRate = new(int)
		*cfg.UninterestingRate = 1
	}
	if cfg.SlowRate == nil {
		cfg.SlowRate = new(int)
		*cfg.SlowRate = 2
	}
	if cfg.HasErrorRate == nil {
		cfg.HasErrorRate = new(int)
		*cfg.HasErrorRate = 2
	}
	if cfg.EstimatorWindowSize == nil {
		cfg.EstimatorWindowSize = new(int)
		*cfg.EstimatorWindowSize = 30
	}
	if cfg.EstimatorInterval == nil {
		cfg.EstimatorInterval = new(int64)
		*cfg.EstimatorInterval = 10_000
	}

	if *cfg.UninterestingRate < 0 {
		err := fmt.Errorf("invalid uninteresting rate: %d, must be >= 0", cfg.UninterestingRate)
		errors = multierror.Append(errors, err)
	}
	if *cfg.SlowRate < 0 {
		err := fmt.Errorf("invalid slow rate: %d, must be >= 0", cfg.SlowRate)
		errors = multierror.Append(errors, err)
	}
	if *cfg.HasErrorRate < 0 {
		err := fmt.Errorf("invalid has error rate: %d, must be >= 0", cfg.HasErrorRate)
		errors = multierror.Append(errors, err)
	}
	return errors.ErrorOrNil()
}

func checkSamplerConfigFile(file string) error {
	u, err := url.Parse(file)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "file" && u.Scheme != "s3" && u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %s, must be 'file', 's3', 'http', or 'https'", u.Scheme)
	}
	return nil
}

func makeConfigurationManager(conf *Config, logger *zap.Logger) (sampler.ConfigManager, error) {
	ctx := context.Background()

	u, err := url.Parse(conf.SamplerConfigFile)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	switch u.Scheme {
	case "file":
		return makeFileConfigManager(u.Path, conf, logger)
	case "s3":
		return makeS3ConfigManager(ctx, u, conf, logger)
	case "http", "https":
		return makeHTTPConfigManager(ctx, u, conf, logger)
	default:
		return nil, fmt.Errorf("unsupported scheme: %s, must be 'file' or 's3", u.Scheme)
	}
}

func makeHTTPConfigManager(_ context.Context, u *url.URL, conf *Config, logger *zap.Logger) (sampler.ConfigManager, error) {
	fr := filereader.NewHTTPFileReader(u.String(), conf.APIKey, nil)
	return sampler.NewConfigManagerImpl(logger, conf.configCheckInterval, fr), nil
}

func makeFileConfigManager(path string, conf *Config, logger *zap.Logger) (sampler.ConfigManager, error) {
	fr := filereader.NewLocalFileReader(path)
	return sampler.NewConfigManagerImpl(logger, conf.configCheckInterval, fr), nil
}

func makeS3ConfigManager(ctx context.Context, u *url.URL, conf *Config, logger *zap.Logger) (sampler.ConfigManager, error) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		return nil, errors.New("AWS_ACCESS_KEY_ID is not set")
	}

	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		return nil, errors.New("AWS_SECRET_ACCESS_KEY is not set")
	}

	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = "auto"
	}

	endpoint := conf.S3Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("S3_ENDPOINT")
	}
	if v := os.Getenv("OBJECTSTORE_ENDPOINT"); v != "" {
		endpoint = v
	}

	provider := conf.S3Provider
	if provider == "" {
		provider = os.Getenv("S3_PROVIDER")
	}

	if provider == "ceph" {
		host := os.Getenv("BUCKET_HOST")
		port := os.Getenv("BUCKET_PORT")
		if host != "" && port != "" {
			endpoint = fmt.Sprintf("http://%s:%s", host, port)
		}
	}

	s3conf := s3tools.Config{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    region,
		Endpoint:  endpoint,
		Provider:  provider,
	}

	s3client, err := s3tools.NewS3Client(ctx, s3conf)
	if err != nil {
		logger.Fatal("Failed to create S3 client", zap.Error(err))
	}

	bucket := strings.SplitN(u.Path, "/", 2)[0]

	fr := filereader.NewS3FileReader(s3client, bucket, u.Path)
	return sampler.NewConfigManagerImpl(logger, conf.configCheckInterval, fr), nil
}
