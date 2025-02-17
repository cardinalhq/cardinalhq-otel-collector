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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		errExpected error
	}{
		{
			name: "valid",
			config: func() *Config {
				return &Config{
					Interval:            2 * time.Second,
					MaximumAge:          2 * time.Minute,
					MetricName:          "test_metric",
					MetricNameAttribute: "test_metric_attribute",
				}
			}(),
			errExpected: nil,
		},
		{
			name: "invalid maximum age",
			config: func() *Config {
				return &Config{
					Interval:            2 * time.Second,
					MaximumAge:          30 * time.Second,
					MetricName:          "test_metric",
					MetricNameAttribute: "test_metric_attribute",
				}
			}(),
			errExpected: fmt.Errorf("maximum_age must be greater than 1 minute"),
		},
		{
			name: "invalid interval",
			config: func() *Config {
				return &Config{
					Interval:            500 * time.Millisecond,
					MaximumAge:          2 * time.Minute,
					MetricName:          "test_metric",
					MetricNameAttribute: "test_metric_attribute",
				}
			}(),
			errExpected: fmt.Errorf("interval must be greater than 1 second"),
		},
		{
			name: "empty metric name",
			config: func() *Config {
				return &Config{
					Interval:            2 * time.Second,
					MaximumAge:          2 * time.Minute,
					MetricName:          "",
					MetricNameAttribute: "test_metric_attribute",
				}
			}(),
			errExpected: fmt.Errorf("metric_name must not be empty"),
		},
		{
			name: "empty metric name attribute",
			config: func() *Config {
				return &Config{
					Interval:            2 * time.Second,
					MaximumAge:          2 * time.Minute,
					MetricName:          "test_metric",
					MetricNameAttribute: "",
				}
			}(),
			errExpected: fmt.Errorf("metric_name_attribute must not be empty"),
		},
		{
			name: "configuration extension set with metrics",
			config: func() *Config {
				return &Config{
					Interval:               2 * time.Second,
					MaximumAge:             2 * time.Minute,
					MetricName:             "test_metric",
					MetricNameAttribute:    "test_metric_attribute",
					ConfigurationExtension: &component.ID{},
					Metrics:                []MetricConfig{{Name: "metric1"}},
				}
			}(),
			errExpected: fmt.Errorf("metrics must be empty when configuration_extension is set"),
		},
		{
			name: "invalid metrics",
			config: func() *Config {
				return &Config{
					Interval:            2 * time.Second,
					MaximumAge:          2 * time.Minute,
					MetricName:          "test_metric",
					MetricNameAttribute: "test_metric_attribute",
					Metrics:             []MetricConfig{{Name: ""}},
				}
			}(),
			errExpected: fmt.Errorf("metric name must not be empty: 0"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.errExpected != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errExpected.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
