// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"errors"
	"strings"

	"go.uber.org/multierr"
)

type customerIDSource int

const (
	customerIDSourceNone customerIDSource = iota
	customerIDSourceMetadata
	customerIDSourceAuth
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

type Metadata struct {
	CustomerIDContextKey string `mapstructure:"customer_id_context_key"`

	customerIDKey string
	customerIDSource
}

type TimeboxConfig struct {
	Interval          int64 `mapstructure:"interval"`
	GracePeriod       int64 `mapstructure:"grace_period"`
	OpenIntervalCount int64 `mapstructure:"open_interval_count"`
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
	Metadata   Metadata         `mapstructure:"metadata"`
	Timeboxes  TimeboxesConfig  `mapstructure:"timeboxes"`
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
	errs = multierr.Append(errs, c.Metadata.Validate())
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

func (m Metadata) Validate() error {
	if m.CustomerIDContextKey != "" {
		parts := strings.Split(m.CustomerIDContextKey, ".")
		if len(parts) < 2 || parts[0] != "metadata" && parts[0] != "auth" {
			return errors.New("customer_id_context_key must be in the format 'metadata.<key>' or 'auth.<key>'")
		}
	}
	return nil
}
