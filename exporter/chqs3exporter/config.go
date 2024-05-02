// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

// S3UploaderConfig contains aws s3 uploader related config to controls things
// like bucket, prefix, batching, connections, retries, etc.
type S3UploaderConfig struct {
	Region           string `mapstructure:"region"`
	S3Bucket         string `mapstructure:"s3_bucket"`
	S3Prefix         string `mapstructure:"s3_prefix"`
	S3Partition      string `mapstructure:"s3_partition"`
	FilePrefix       string `mapstructure:"file_prefix"`
	Endpoint         string `mapstructure:"endpoint"`
	RoleArn          string `mapstructure:"role_arn"`
	S3ForcePathStyle bool   `mapstructure:"s3_force_path_style"`
	DisableSSL       bool   `mapstructure:"disable_ssl"`
}

type TimeboxConfig struct {
	Interval    int64 `mapstructure:"interval"`
	GracePeriod int64 `mapstructure:"grace_period"`
}

// TimeboxConfig contains the configuration for the timebox
type TimeboxesConfig struct {
	Logs    TimeboxConfig `mapstructure:"logs"`
	Metrics TimeboxConfig `mapstructure:"metrics"`
	Traces  TimeboxConfig `mapstructure:"traces"`
}

// Config contains the main configuration options for the s3 exporter
type Config struct {
	S3Uploader S3UploaderConfig `mapstructure:"s3uploader"`

	Timeboxes TimeboxesConfig `mapstructure:"timeboxes"`

	// Encoding to apply. If present, overrides the marshaler configuration option.
	Encoding              *component.ID `mapstructure:"encoding"`
	EncodingFileExtension string        `mapstructure:"encoding_file_extension"`
}

func (c *Config) Validate() error {
	var errs error
	if c.S3Uploader.Region == "" {
		errs = multierr.Append(errs, errors.New("region is required"))
	}
	if c.S3Uploader.S3Bucket == "" {
		errs = multierr.Append(errs, errors.New("bucket is required"))
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

	return errs
}
