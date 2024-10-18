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

package chqdatadogexporter

import (
	"errors"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueConfig                  exporterhelper.QueueConfig `mapstructure:"sending_queue"`
	RetryConfig                  configretry.BackOffConfig  `mapstructure:"retry_on_failure"`
	APIKey                       configopaque.String        `mapstructure:"api_key"`
	Metrics                      MetricsConfig              `mapstructure:"metrics"`
	Logs                         LogsConfig                 `mapstructure:"logs"`
	Traces                       TracesConfig               `mapstructure:"traces"`
}

var errAPIKeyMissing = errors.New("api_key must be specified")

func (c *Config) Validate() error {
	if c.APIKey == "" {
		return errAPIKeyMissing
	}
	if c.Metrics.APIKey == "" {
		c.Metrics.APIKey = c.APIKey
	}
	if c.Logs.APIKey == "" {
		c.Logs.APIKey = c.APIKey
	}
	if c.Traces.APIKey == "" {
		c.Traces.APIKey = c.APIKey
	}
	return nil
}

type MetricsConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	APIKey                  configopaque.String `mapstructure:"api_key"`
}

type LogsConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	APIKey                  configopaque.String `mapstructure:"api_key"`
}

type TracesConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	APIKey                  configopaque.String `mapstructure:"api_key"`
}
