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

package chqstatsprocessor

import (
	"errors"
	"time"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"go.opentelemetry.io/collector/config/confighttp"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Statistics              StatisticsConfig                `mapstructure:"statistics"`
	ConfigurationExtension  *component.ID                   `mapstructure:"configuration_extension"`
	IDSource                authenv.EnvironmentSourceString `mapstructure:"id_source"`
}

type StatisticsConfig struct {
	Interval time.Duration        `mapstructure:"interval"`
	Phase    string               `mapstructure:"phase"`
	Metrics  StatisticsTypeConfig `mapstructure:"metrics"`
	Logs     StatisticsTypeConfig `mapstructure:"logs"`
	Traces   StatisticsTypeConfig `mapstructure:"traces"`
}

type StatisticsTypeConfig struct {
	StatisticsEnabled bool `mapstructure:"statistics_enabled"`
	ExemplarsEnabled  bool `mapstructure:"exemplars_enabled"`
}

type ContextID = string

func (c *Config) Validate() error {
	var errs error

	if _, err := authenv.ParseEnvironmentSource(c.IDSource); err != nil {
		errs = multierr.Append(errs, errors.New("id_source must be a valid environment source: "+err.Error()))
	}

	errs = multierr.Append(errs, c.Statistics.Validate())

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
