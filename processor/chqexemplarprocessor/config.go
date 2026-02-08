// Copyright 2024-2025 CardinalHQ, Inc
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

package chqexemplarprocessor

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Reporting               ReportingConfig `mapstructure:"reporting"`
}

type ReportingConfig struct {
	Metrics EnabledOption `mapstructure:"metrics"`
	Logs    EnabledOption `mapstructure:"logs"`
	Traces  EnabledOption `mapstructure:"traces"`
}

type EnabledOption struct {
	Enabled   bool          `mapstructure:"enabled"`
	Interval  time.Duration `mapstructure:"interval"`
	Expiry    time.Duration `mapstructure:"expiry"`
	CacheSize int           `mapstructure:"cache_size"`
	BatchSize int           `mapstructure:"batch_size"`
}

type ContextID = string

func (c *Config) Validate() error {
	var errs error

	if c.Endpoint == "" {
		errs = multierr.Append(errs, errors.New("endpoint must be specified"))
	}

	errs = multierr.Append(errs, c.Reporting.Validate())

	return errs
}

func (c *ReportingConfig) Validate() error {
	var errs error

	errs = multierr.Append(errs, c.Metrics.Validate())
	errs = multierr.Append(errs, c.Logs.Validate())
	errs = multierr.Append(errs, c.Traces.Validate())

	return errs
}

func (e *EnabledOption) Validate() error {
	if e.Interval <= 0 {
		return errors.New("interval must be positive")
	}

	if e.Expiry <= 0 {
		return errors.New("expiry must be positive")
	}

	if e.Expiry < e.Interval*2 {
		return errors.New("expiry must be at least twice the interval")
	}

	if e.CacheSize < 1000 {
		return errors.New("LRU cache size must be at least 1000")
	}

	return nil
}
