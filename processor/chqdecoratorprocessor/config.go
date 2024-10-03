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

package chqdecoratorprocessor

import (
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Config struct {
	LogsConfig   LogsConfig   `mapstructure:"logs"`
	TracesConfig TracesConfig `mapstructure:"traces"`
}

type LogsConfig struct {
	Transforms []ContextStatement `mapstructure:"log_statements"`
}

type TracesConfig struct {
	Transforms          []ContextStatement `mapstructure:"trace_statements"`
	EstimatorWindowSize int                `mapstructure:"estimator_window_size"`
	EstimatorInterval   int64              `mapstructure:"estimator_interval"`
}

type ContextID string

type ContextStatement struct {
	Context    ContextID `mapstructure:"context"`
	Conditions []string  `mapstructure:"conditions"`
	Statements []string  `mapstructure:"statements"`
}

var _ component.Config = (*Config)(nil)

var validContexts = map[ContextID]bool{
	"resource": true,
	"scope":    true,
	"log":      true,
}

// Validate function for your custom processor's Config
func (c *Config) Validate() error {
	var errors error
	if len(c.LogsConfig.Transforms) > 0 {
		pc, err := ottllog.NewParser(ottlfuncs.StandardFuncs[ottllog.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
		if err != nil {
			return err
		}

		for _, cs := range c.LogsConfig.Transforms {
			// Check if ContextID is valid (resource, scope, or log)
			if !validContexts[cs.Context] {
				err := fmt.Errorf("invalid context: %s. Must be one of: resource, scope, log", cs.Context)
				errors = multierr.Append(errors, err)
				continue
			}

			// Parse the statements if the context is valid
			_, err = pc.ParseStatements(cs.Statements)
			if err != nil {
				errors = multierr.Append(errors, err)
			}

			// Parse the conditions if the context is valid
			_, err = pc.ParseConditions(cs.Conditions)
			if err != nil {
				errors = multierr.Append(errors, err)
			}
		}
	}

	multierr.Append(errors, validateTracesConfig(c.TracesConfig))

	return errors
}

func validateTracesConfig(tc TracesConfig) error {
	var errors error
	if tc.EstimatorWindowSize < 10 {
		err := fmt.Errorf("estimator_window_size must be greater than or equal to 10")
		errors = multierr.Append(errors, err)
	}

	if tc.EstimatorInterval < 1000 {
		err := fmt.Errorf("estimator_interval must be greater than or equal to 1000")
		errors = multierr.Append(errors, err)
	}

	return errors
}
