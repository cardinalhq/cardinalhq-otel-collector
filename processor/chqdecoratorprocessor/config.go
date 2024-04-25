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

	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/filereader"
	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/s3tools"
	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/sampler"
)

type Config struct {
	SampleConfigFile    string `mapstructure:"sample_config_file"`
	Region              string `mapstructure:"region"`
	S3Endpoint          string `mapstructure:"s3_endpoint"`
	S3Provider          string `mapstructure:"s3_provider"`
	ConfigCheckInterval int    `mapstructure:"config_check_interval"`
	APIKey              string `mapstructure:"api_key"`

	configCheckInterval time.Duration
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {
	if cfg.configCheckInterval <= 0 {
		cfg.configCheckInterval = 10 * time.Second
	}
	cfg.configCheckInterval = time.Duration(cfg.ConfigCheckInterval) * time.Second

	if cfg.SampleConfigFile != "" {
		return checkSamplerConfigFile(cfg.SampleConfigFile)
	}
	return nil
}

func checkSamplerConfigFile(file string) error {
	u, err := url.Parse(file)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "file" && u.Scheme != "s3" {
		return fmt.Errorf("unsupported scheme: %s, must be 'file' or 's3", u.Scheme)
	}
	return nil
}

func makeConfigurationManager(conf *Config, logger *zap.Logger) (sampler.ConfigManager, error) {
	ctx := context.Background()

	u, err := url.Parse(conf.SampleConfigFile)
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
