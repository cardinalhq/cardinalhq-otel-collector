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

package chqmissingdataconnector

import (
	"errors"
	"fmt"
	"time"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
)

var (
	defaultMaximumAge      = 24 * time.Hour
	defaultInterval        = 1 * time.Minute
	defaultResourcesToCopy = []string{
		"k8s.cluster.name",
		"k8s.namespace.name",
		"service.name",
	}
	defaultMetricName          = "missingdata.age"
	defaultMetricNameAttribute = "missingdata.metric.name"

	errMaximumAgeTooSmall           = errors.New("maximum_age must be greater than 1 minute")
	errIntervalTooSmall             = errors.New("interval must be greater than 1 second")
	errMetricNameEmpty              = errors.New("metric_name must not be empty")
	errMetricNameAttributeEmpty     = errors.New("metric_name_attribute must not be empty")
	errMetricsNotEmptyWithExtension = errors.New("metrics must be empty when configuration_extension is set")
	errMetricNameDuplicate          = "duplicate metric name: %s"
	errMetricNameEmptyInList        = "metric name must not be empty: %d"
)

// Config defines configuration for the CardinalHQ Missing Data Connector.
type Config struct {
	ConfigurationExtension   *component.ID  `mapstructure:"configuration_extension"`
	Interval                 time.Duration  `mapstructure:"interval"`
	MaximumAge               time.Duration  `mapstructure:"maximum_age"`
	MetricName               string         `mapstructure:"metric_name"`
	MetricNameAttribute      string         `mapstructure:"metric_name_attribute"`
	Metrics                  []MetricConfig `mapstructure:"metrics"`
	ResourceAttributesToCopy []string       `mapstructure:"resource_attributes_to_copy"`
}

// MetricConfig defines configuration for a metric.
type MetricConfig struct {
	Name               string   `mapstructure:"name"`
	Attributes         []string `mapstructure:"attributes"`
	ResourceAttributes []string `mapstructure:"resource_attributes"`
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	var errs error

	if c.MaximumAge <= 1*time.Minute {
		errs = multierr.Append(errs, errMaximumAgeTooSmall)
	}
	if c.Interval <= 1*time.Second {
		errs = multierr.Append(errs, errIntervalTooSmall)
	}
	if c.MetricName == "" {
		errs = multierr.Append(errs, errMetricNameEmpty)
	}
	if c.MetricNameAttribute == "" {
		errs = multierr.Append(errs, errMetricNameAttributeEmpty)
	}

	if c.ConfigurationExtension == nil {
		if err := c.validateMetrics(); err != nil {
			errs = multierr.Append(errs, err)
		}
	} else {
		if len(c.Metrics) > 0 {
			errs = multierr.Append(errs, errMetricsNotEmptyWithExtension)
		}
		c.ResourceAttributesToCopy = append(c.ResourceAttributesToCopy, translate.CardinalFieldCustomerID)
	}

	return errs
}

func (c *Config) validateMetrics() error {
	var errs error

	foundNames := make(map[string]struct{}, len(c.Metrics))
	for i, metric := range c.Metrics {
		if metric.Name == "" {
			errs = multierr.Append(errs, fmt.Errorf(errMetricNameEmptyInList, i))
			continue
		}
		if _, found := foundNames[metric.Name]; found {
			errs = multierr.Append(errs, fmt.Errorf(errMetricNameDuplicate, metric.Name))
		}
	}

	return errs
}
