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
	"fmt"
	"net/url"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	// ClusterID is the ID of the cluster to which the collector belongs.
	// All collectors able to receive the same data should use this same value.
	ClusterID string `mapstructure:"cluster_id"`

	MetricConfig MetricConfig `mapstructure:"metrics"`

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
	if cfg.MetricConfig.MetricAggregationInterval < 10 {
		return fmt.Errorf("invalid metric aggregation interval: %d, must be >= than 10", cfg.MetricConfig.MetricAggregationInterval)
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
	if u.Scheme != "file" && u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %s, must be 'file', 'http', or 'https'", u.Scheme)
	}
	return nil
}
