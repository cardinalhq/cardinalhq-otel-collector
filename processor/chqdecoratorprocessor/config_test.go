// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package chqdecoratorprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test the Validate function for Config
func TestConfigValidate(t *testing.T) {
	// Case 1: Valid Config with valid Transforms
	validConfig := &Config{
		LogsConfig: LogsConfig{
			Transforms: []ContextStatement{
				{
					Context:    "log",
					Conditions: []string{"IsMap(body) and body[\"object\"] != nil"},
					Statements: []string{"set(body, attributes[\"http.route\"])"},
				},
			},
		},
	}

	err := validConfig.Validate()
	assert.NoError(t, err, "Expected no error for valid config")

	// Case 2: Invalid Config with invalid Transforms (invalid condition)
	invalidConfig := &Config{
		LogsConfig: LogsConfig{
			Transforms: []ContextStatement{
				{
					Context:    "log",
					Conditions: []string{"IsMap(body) and body[object] != nil"}, // Invalid condition (unquoted key)
					Statements: []string{"set(body, attributes[\"http.route\"])"},
				},
			},
		},
	}

	err = invalidConfig.Validate()
	assert.Error(t, err, "Expected error for invalid condition in Transforms")

	// Case 3: Invalid Config with invalid Statements
	invalidStatementConfig := &Config{
		LogsConfig: LogsConfig{
			Transforms: []ContextStatement{
				{
					Context:    "log",
					Conditions: []string{"IsMap(body) and body[\"object\"] != nil"},
					Statements: []string{"set(log.attributes['new_key' = 'new_value'])"}, // Invalid statement syntax
				},
			},
		},
	}

	err = invalidStatementConfig.Validate()
	assert.Error(t, err, "Expected error for invalid statement in Transforms")

	// Case 4: Empty Config (should pass with no error)
	emptyConfig := &Config{}
	err = emptyConfig.Validate()
	assert.NoError(t, err, "Expected no error for empty config")
}
