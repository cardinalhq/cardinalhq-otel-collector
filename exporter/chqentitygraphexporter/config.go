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

package chqentitygraphexporter

import (
	"errors"
	"time"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"go.opentelemetry.io/collector/config/confighttp"

	"go.uber.org/multierr"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Reporting               ReportingConfig                 `mapstructure:"reporting"`
	IDSource                authenv.EnvironmentSourceString `mapstructure:"id_source"`
}

type ReportingConfig struct {
	Interval time.Duration `mapstructure:"interval"`
}

type ContextID = string

func (c *Config) Validate() error {
	var errs error

	if _, err := authenv.ParseEnvironmentSource(c.IDSource); err != nil {
		errs = multierr.Append(errs, errors.New("id_source must be a valid environment source: "+err.Error()))
	}

	errs = multierr.Append(errs, c.Reporting.Validate())

	return errs
}

func (c *ReportingConfig) Validate() error {
	var errs error
	if c.Interval == 0 {
		c.Interval = defaultReportingInterval
	}

	if c.Interval < 0 {
		errs = multierr.Append(errs, errors.New("interval must be greater than or equal to 0"))
	}

	return errs
}
