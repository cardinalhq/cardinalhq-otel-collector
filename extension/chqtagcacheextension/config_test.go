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

package chqtagcacheextension

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
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://example.com",
				},
				TTL:      300 * time.Second,
				ErrorTTL: 60 * time.Second,
			},
			errExpected: nil,
		},
		{
			name: "check TTL too short",
			config: &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://example.com",
				},
				TTL:      1 * time.Second,
				ErrorTTL: 60 * time.Second,
			},
			errExpected: errTTLTooShort,
		},
		{
			name: "error TTL too short",
			config: &Config{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://example.com",
				},
				TTL:      300 * time.Second,
				ErrorTTL: 1 * time.Second,
			},
			errExpected: errErrorTTLTooShort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}
