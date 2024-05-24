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

package chqstatsexporter

import (
	"errors"
	"time"

	"go.uber.org/multierr"
)

type TimeboxConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

// TimeboxConfig contains the configuration for the timebox
type TimeboxesConfig struct {
	Logs    TimeboxConfig `mapstructure:"logs"`
	Metrics TimeboxConfig `mapstructure:"metrics"`
	Traces  TimeboxConfig `mapstructure:"traces"`
}

// Config contains the main configuration options for the s3 exporter
type Config struct {
	Timeboxes TimeboxesConfig `mapstructure:"timeboxes"`
}

func (c *Config) Validate() error {
	var errs error
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

	if tb.Interval == 0 {
		tb.Interval = time.Second * 10
	}

	if tb.Interval < 0 {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 0"))
	}

	return errs
}
