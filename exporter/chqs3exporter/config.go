// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"errors"
	"os"
	"time"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"go.uber.org/multierr"
)

// S3UploaderConfig contains aws s3 uploader related config to controls things
// like bucket, prefix, batching, connections, retries, etc.
type S3UploaderConfig struct {
	Region           string `mapstructure:"region"`
	S3Bucket         string `mapstructure:"s3_bucket"`
	S3Prefix         string `mapstructure:"s3_prefix"`
	S3Partition      string `mapstructure:"s3_partition"`
	CustomerKey      string `mapstructure:"customer_key"`
	FilePrefix       string `mapstructure:"file_prefix"`
	Endpoint         string `mapstructure:"endpoint"`
	RoleArn          string `mapstructure:"role_arn"`
	S3ForcePathStyle bool   `mapstructure:"s3_force_path_style"`
	DisableSSL       bool   `mapstructure:"disable_ssl"`
}

type TimeboxConfig struct {
	Interval          time.Duration `mapstructure:"interval"`
	GracePeriod       time.Duration `mapstructure:"grace_period"`
	OpenIntervalCount int64         `mapstructure:"open_interval_count"`
}

// TimeboxConfig contains the configuration for the timebox
type TimeboxesConfig struct {
	Logs    TimeboxConfig `mapstructure:"logs"`
	Metrics TimeboxConfig `mapstructure:"metrics"`
	Traces  TimeboxConfig `mapstructure:"traces"`
}

type BufferingConfig struct {
	Type      string `mapstructure:"type"`
	Directory string `mapstructure:"directory"`
}

const (
	bufferTypeDisk   = "disk"
	bufferTypeMemory = "memory"
)

// Config contains the main configuration options for the s3 exporter
type Config struct {
	S3Uploader       S3UploaderConfig                `mapstructure:"s3uploader"`
	Timeboxes        TimeboxesConfig                 `mapstructure:"timeboxes"`
	UseNowForMetrics bool                            `mapstructure:"use_now_for_metrics"`
	Buffering        BufferingConfig                 `mapstructure:"buffering"`
	IDSource         authenv.EnvironmentSourceString `mapstructure:"id_source"`
}

func (c *Config) Validate() error {
	var errs error
	if c.S3Uploader.Region == "" {
		errs = multierr.Append(errs, errors.New("region is required"))
	}
	if c.S3Uploader.S3Bucket == "" {
		errs = multierr.Append(errs, errors.New("bucket is required"))
	}

	if _, err := authenv.ParseEnvironmentSource(c.IDSource); err != nil {
		errs = multierr.Append(errs, errors.New("id_source must be a valid environment source: "+err.Error()))
	}

	errs = multierr.Append(errs, c.Timeboxes.Validate())
	return errs
}

func (tb TimeboxesConfig) Validate() error {
	var errs error

	errs = multierr.Append(errs, tb.Logs.Validate())
	errs = multierr.Append(errs, tb.Metrics.Validate())
	errs = multierr.Append(errs, tb.Traces.Validate())

	return errs
}

func (tb TimeboxConfig) Validate() error {
	var errs error

	if tb.Interval < 0 {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 0"))
	}

	if tb.Interval > 0 && tb.GracePeriod < 0 {
		errs = multierr.Append(errs, errors.New("grace period must be greater than or equal to 0"))
	}

	if tb.Interval > 0 && tb.OpenIntervalCount < 1 {
		errs = multierr.Append(errs, errors.New("open interval count must be greater than or equal to 1"))
	}

	return errs
}

func (c BufferingConfig) Validate() error {
	var errs error

	if c.Type != "" && c.Type != bufferTypeMemory && c.Type != bufferTypeDisk {
		errs = multierr.Append(errs, errors.New("type must be either "+bufferTypeMemory+" or "+bufferTypeDisk+" (default is "+bufferTypeMemory+")"))
	}

	if c.Type == bufferTypeMemory {
		return nil
	}

	if c.Directory == "" {
		errs = multierr.Append(errs, errors.New("directory is required when type is disk"))
	}

	//err := emptyOldFiles(c.Directory)
	//if err != nil {
	//	errs = multierr.Append(errs, err)
	//}
	err := testWritable(c.Directory)
	if err != nil {
		errs = multierr.Append(errs, err)
	}

	return errs
}

func testWritable(dir string) error {
	file, err := os.CreateTemp(dir, "test")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	return nil
}
