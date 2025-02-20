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

package aggregationprocessor

import (
	"errors"
	"time"

	"go.uber.org/multierr"
)

type Config struct {
	MetricAggregation    MetricAggregationConfig `mapstructure:"metric_aggregation"`
	AdditionalAttributes map[string]string       `mapstructure:"additional_attributes"`
}

type MetricAggregationConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

func (c *Config) Validate() error {
	var errs error
	errs = multierr.Append(errs, c.MetricAggregation.Validate())
	return errs
}

func (c *MetricAggregationConfig) Validate() error {
	var errs error

	if c.Interval < 1*time.Second {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 1s"))
	}

	return errs
}
