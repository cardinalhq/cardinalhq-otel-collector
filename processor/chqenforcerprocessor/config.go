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
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

type Config struct {
	Statistics               StatisticsConfig        `mapstructure:"statistics"`
	MetricAggregation        MetricAggregationConfig `mapstructure:"metric_aggregation"`
	TraceConfig              TraceConfig             `mapstructure:"traces"`
	ConfigurationExtension   *component.ID           `mapstructure:"configuration_extension"`
	DropDecorationAttributes bool                    `mapstructure:"drop_decoration_attributes"`
}

type StatisticsConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Interval                time.Duration `mapstructure:"interval"`
	Phase                   string        `mapstructure:"phase"`
	Vendor                  string        `mapstructure:"vendor"`
}

type MetricAggregationConfig struct {
	Interval time.Duration `mapstructure:"interval"`
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

func (c *Config) Validate() error {
	var errs error

	if c.ConfigurationExtension == nil {
		errs = multierr.Append(errs, errors.New("configuration_extension is required"))
	}
	errs = multierr.Append(errs, c.Statistics.Validate())
	errs = multierr.Append(errs, c.MetricAggregation.Validate())
	errs = multierr.Append(errs, c.TraceConfig.Validate())

	return errs
}

func (c *StatisticsConfig) Validate() error {
	var errs error
	if c.Interval == 0 {
		c.Interval = 5 * time.Minute
	}

	if c.Interval < 0 {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 0"))
	}

	if c.Phase != "presample" && c.Phase != "postsample" {
		errs = multierr.Append(errs, errors.New("phase must be either presample or postsample, not "+c.Phase))
	}

	return errs
}

func (c *MetricAggregationConfig) Validate() error {
	var errs error

	if c.Interval < 1*time.Second {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 1s"))
	}

	return errs
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
		cfg.UninterestingRate = &defaultUninterestingRate
	}
	if cfg.SlowRate == nil {
		cfg.SlowRate = &defaultSlowRate
	}
	if cfg.HasErrorRate == nil {
		cfg.HasErrorRate = &defaultHasErrorRate
	}
	if cfg.EstimatorWindowSize == nil {
		cfg.EstimatorWindowSize = &defaultEstimatorWindowSize
	}
	if cfg.EstimatorInterval == nil {
		cfg.EstimatorInterval = &defaultEstimatorInterval
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
