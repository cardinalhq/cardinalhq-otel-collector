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

package extractmetricsprocessor

import (
	"errors"

	"go.opentelemetry.io/collector/config/confighttp"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

const (
	// gaugeDoubleType is the gauge double metric type.
	gaugeDoubleType = "gauge_double"

	// gaugeIntType is the gauge int metric type.
	gaugeIntType = "gauge_int"

	// counterDoubleType is the counter float metric type.
	counterDoubleType = "counter_double"

	// counterIntType is the counter int metric type.
	counterIntType = "counter_int"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Route                   string        `mapstructure:"route"`
	ConfigurationExtension  *component.ID `mapstructure:"configuration_extension"`
}

func (c *Config) Validate() error {
	var errs error

	if c.ConfigurationExtension == nil {
		errs = multierr.Append(errs, errors.New("configuration_extension is required"))
	}

	return errs
}
