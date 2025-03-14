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

package chqk8smetricsconnector

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

var (
	defaultEventsReportingInterval = 5 * time.Minute

	errInvalidEventsReportingInterval = fmt.Errorf("events.interval must be greater than or equal to 1m")
	errInvalidEndpoint                = fmt.Errorf("events.endpoint must be specified")
)

// Config defines configuration for the CardinalHQ Kubernetes Metrics Connector.
type Config struct {
	Events EventsConfig `mapstructure:"events"`
}

type EventsConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Interval                time.Duration `mapstructure:"interval"`
}

func (c *Config) Validate() error {
	var errs error

	errs = multierr.Append(errs, c.Events.validate())

	return errs
}

func (c *EventsConfig) validate() error {
	var errs error

	if c.Interval < 1*time.Minute {
		errs = multierr.Append(errs, errInvalidEventsReportingInterval)
	}

	if c.Endpoint == "" {
		errs = multierr.Append(errs, errInvalidEndpoint)
	}

	return errs
}
