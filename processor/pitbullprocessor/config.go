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

package pitbullprocessor

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

type Config struct {
	MetricAggregation      MetricAggregationConfig `mapstructure:"metric_aggregation"`
	LogsConfig             LogsConfig              `mapstructure:"logs"`
	TracesConfig           TracesConfig            `mapstructure:"traces"`
	MetricsConfig          MetricsConfig           `mapstructure:"metrics"`
	ConfigurationExtension *component.ID           `mapstructure:"configuration_extension"`
}

type MetricAggregationConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

type ContextID string

type StatsEnrichment struct {
	Context ContextID `mapstructure:"context"`
	Tags    []string  `mapstructure:"tags"`
}

type LogsConfig struct {
	StatsEnrichments []StatsEnrichment `mapstructure:"stats_enrichments"`
}

type MetricsConfig struct {
	StatsEnrichments []StatsEnrichment `mapstructure:"stats_enrichments"`
}

type TracesConfig struct {
	StatsEnrichments    []StatsEnrichment `mapstructure:"stats_enrichments"`
	EstimatorWindowSize int               `mapstructure:"estimator_window_size"`
	EstimatorInterval   int64             `mapstructure:"estimator_interval"`
}

func (c *Config) Validate() error {
	var errs error

	if c.ConfigurationExtension == nil {
		errs = multierr.Append(errs, errors.New("configuration_extension is required"))
	}
	errs = multierr.Append(errs, c.MetricAggregation.Validate())
	errs = multierr.Append(errs, c.LogsConfig.Validate())
	errs = multierr.Append(errs, c.TracesConfig.Validate())

	return errs
}

func (c *MetricAggregationConfig) Validate() error {
	var errs error

	if c.Interval < 1*time.Second {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 1s"))
	}

	return errs
}

var validContexts = map[ContextID]bool{
	"resource": true,
	"scope":    true,
	"log":      true,
}

// Validate function for LogsConfig
func (cfg *LogsConfig) Validate() error {
	var errors error

	// Validate each StatsEnrichment
	for _, enrichment := range cfg.StatsEnrichments {
		// Check if the ContextID is valid
		if !validContexts[enrichment.Context] {
			// Append error for invalid context and skip tags validation for this entry
			err := fmt.Errorf("invalid context: %s. Must be one of: resource, scope, log", enrichment.Context)
			errors = multierr.Append(errors, err)
			continue
		}

		// Validate Tags only if ContextID is valid
		if len(enrichment.Tags) == 0 {
			err := fmt.Errorf("empty tags for context: %s", enrichment.Context)
			errors = multierr.Append(errors, err)
		}
	}

	return errors
}

func (tc *TracesConfig) Validate() error {
	var errors error
	if tc.EstimatorWindowSize == 0 {
		tc.EstimatorWindowSize = 30
	}
	if tc.EstimatorWindowSize < 10 {
		err := fmt.Errorf("estimator_window_size must be greater than or equal to 10")
		errors = multierr.Append(errors, err)
	}

	if tc.EstimatorInterval == 0 {
		tc.EstimatorInterval = 10_000
	}
	if tc.EstimatorInterval < 1000 {
		err := fmt.Errorf("estimator_interval must be greater than or equal to 1000")
		errors = multierr.Append(errors, err)
	}

	return errors
}
