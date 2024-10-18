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

package chqstatsprocessor

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

type Config struct {
	Statistics StatisticsConfig `mapstructure:"statistics"`
}

type StatisticsConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`

	Interval time.Duration `mapstructure:"interval"`
	Phase    string        `mapstructure:"phase"`
	Vendor   string        `mapstructure:"vendor"`

	LogsEnrichments    []StatsEnrichment `mapstructure:"logs_enrichments"`
	MetricsEnrichments []StatsEnrichment `mapstructure:"metrics_enrichments"`
	TracesEnrichments  []StatsEnrichment `mapstructure:"traces_enrichments"`
}

type ContextID = string

type StatsEnrichment struct {
	Context ContextID `mapstructure:"context"`
	Tags    []string  `mapstructure:"tags"`
}

func (c *Config) Validate() error {
	var errs error

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
