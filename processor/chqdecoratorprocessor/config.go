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
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

type Config struct {
	Statistics             StatisticsConfig `mapstructure:"statistics"`
	TracesConfig           TracesConfig     `mapstructure:"traces"`
	ConfigurationExtension *component.ID    `mapstructure:"configuration_extension"`
}

type StatisticsConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Interval                time.Duration `mapstructure:"interval"`
	Phase                   string        `mapstructure:"phase"`
	Vendor                  string        `mapstructure:"vendor"`
}

type TracesConfig struct {
	EstimatorWindowSize int   `mapstructure:"estimator_window_size"`
	EstimatorInterval   int64 `mapstructure:"estimator_interval"`
}

var _ component.Config = (*Config)(nil)

// Validate function for your custom processor's Config
func (c *Config) Validate() error {
	return validateTracesConfig(c.TracesConfig)
}

func validateTracesConfig(tc TracesConfig) error {
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
