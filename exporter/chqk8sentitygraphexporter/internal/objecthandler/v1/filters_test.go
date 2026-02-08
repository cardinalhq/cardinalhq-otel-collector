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

package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func TestIsFiltredConfigMapName(t *testing.T) {
	tests := []struct {
		name       string
		config     *converterconfig.Config
		configName string
		expected   bool
	}{
		{
			name: "Name matches one of the ignored names",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: []converterconfig.StringMatcher{
					converterconfig.NewLiteralMatcher("ignored-configmap"),
				},
			},
			configName: "ignored-configmap",
			expected:   true,
		},
		{
			name: "Name does not match any ignored names",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: []converterconfig.StringMatcher{
					converterconfig.NewLiteralMatcher("ignored-configmap"),
				},
			},
			configName: "non-ignored-configmap",
			expected:   false,
		},
		{
			name: "No ignored names configured",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: []converterconfig.StringMatcher{},
			},
			configName: "any-configmap",
			expected:   false,
		},
		{
			name: "Nil ignored names configured",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: nil,
			},
			configName: "any-configmap",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isFilteredConfigMapName(tt.config, tt.configName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsFiltredSecretName(t *testing.T) {
	tests := []struct {
		name       string
		config     *converterconfig.Config
		secretName string
		expected   bool
	}{
		{
			name: "Name matches one of the ignored names",
			config: &converterconfig.Config{
				IgnoredSecretNames: []converterconfig.StringMatcher{
					converterconfig.NewLiteralMatcher("ignored-secret"),
				},
			},
			secretName: "ignored-secret",
			expected:   true,
		},
		{
			name: "Name does not match any ignored names",
			config: &converterconfig.Config{
				IgnoredSecretNames: []converterconfig.StringMatcher{
					converterconfig.NewLiteralMatcher("ignored-secret"),
				},
			},
			secretName: "non-ignored-secret",
			expected:   false,
		},
		{
			name: "No ignored names configured",
			config: &converterconfig.Config{
				IgnoredSecretNames: []converterconfig.StringMatcher{},
			},
			secretName: "any-secret",
			expected:   false,
		},
		{
			name: "Nil ignored names configured",
			config: &converterconfig.Config{
				IgnoredSecretNames: nil,
			},
			secretName: "any-secret",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isFilteredSecretName(tt.config, tt.secretName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
