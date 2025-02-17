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
		"service.name",
		"k8s.cluster.name",
		"k8s.pod.name",
		"k8s.node.name",
		"k8s.namespace.name",
		"k8s.deployment.name",
		"k8s.daemonset.name",
		"k8s.statefulset.name",
		"k8s.replicaset.name",
		"k8s.job.name",
		"k8s.cronjob.name",
	}
	defaultMetricName          = "missingdata.age"
	defaultMetricNameAttribute = "missingdata.metric.name"
)

type Config struct {
	ConfigurationExtension   *component.ID  `mapstructure:"configuration_extension"`
	Interval                 time.Duration  `mapstructure:"interval"`
	MaximumAge               time.Duration  `mapstructure:"maximum_age"`
	MetricName               string         `mapstructure:"metric_name"`
	MetricNameAttribute      string         `mapstructure:"metric_name_attribute"`
	Metrics                  []MetricConfig `mapstructure:"metrics"`
	ResourceAttributesToCopy []string       `mapstructure:"resource_attributes_to_copy"`
}

type MetricConfig struct {
	Name               string   `mapstructure:"name"`
	Attributes         []string `mapstructure:"attributes"`
	ResourceAttributes []string `mapstructure:"resource_attributes"`
}

func (c *Config) Validate() error {
	var errs error

	if c.MaximumAge <= 1*time.Minute {
		errs = multierr.Append(errs, fmt.Errorf("maximum_age must be greater than 1 minute"))
	}
	if c.Interval <= 1*time.Second {
		errs = multierr.Append(errs, fmt.Errorf("interval must be greater than 1 second"))
	}
	if c.MetricName == "" {
		errs = multierr.Append(errs, fmt.Errorf("metric_name must not be empty"))
	}
	if c.MetricNameAttribute == "" {
		errs = multierr.Append(errs, fmt.Errorf("metric_name_attribute must not be empty"))
	}

	if c.ConfigurationExtension == nil {
		if err := c.validateMetrics(); err != nil {
			errs = multierr.Append(errs, err)
		}
	} else {
		if len(c.Metrics) > 0 {
			errs = multierr.Append(errs, fmt.Errorf("metrics must be empty when configuration_extension is set"))
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
			errs = multierr.Append(errs, fmt.Errorf("metric name must not be empty: %d", i))
			continue
		}
		if _, found := foundNames[metric.Name]; found {
			errs = multierr.Append(errs, fmt.Errorf("duplicate metric name: %s", metric.Name))
		}
	}

	return errs
}
