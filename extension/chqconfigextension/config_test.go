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

package chqconfigextension

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/confighttp"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		errExpected error
	}{
		{
			name: "valid",
			config: &Config{
				Source: ConfigSourceConfig{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://example.com",
					},
				},
				CheckInterval: 2 * time.Minute,
			},
			errExpected: nil,
		},
		{
			name: "check interval too short",
			config: &Config{
				Source: ConfigSourceConfig{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://example.com",
					},
				},
				CheckInterval: 3 * time.Second,
			},
			errExpected: errCheckIntervalTooShort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}

func TestConfigSourceConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *ConfigSourceConfig
		errExpected error
	}{
		{
			name: "valid",
			config: &ConfigSourceConfig{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://example.com",
				},
			},
			errExpected: nil,
		},
		{
			name: "missing endpoint",
			config: &ConfigSourceConfig{
				ClientConfig: confighttp.ClientConfig{},
			},
			errExpected: errMissingEndpoint,
		},
		{
			name: "invalid endpoint",
			config: &ConfigSourceConfig{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: ":not-a-url:::",
				},
			},
			errExpected: errInvalidEndpoint,
		},
		{
			name: "invalid scheme",
			config: &ConfigSourceConfig{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "ftp://example.com",
				},
			},
			errExpected: errInvalidScheme,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}
