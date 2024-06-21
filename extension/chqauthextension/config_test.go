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

package chqauthextension

import (
	"testing"

	"go.opentelemetry.io/collector/config/confighttp"
)

func TestServerAuth_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *ServerAuth
		expected    *ServerAuth
		errExpected error
	}{
		{
			"valid",
			&ServerAuth{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
			},
			&ServerAuth{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
				CacheTTLValid:   defaultCacheValidTTL,
				CacheTTLInvalid: defaultCacheInvalidTTL,
			},
			nil,
		},
		{
			"invalid",
			&ServerAuth{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "",
				},
			},
			&ServerAuth{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "",
				},
				CacheTTLValid:   defaultCacheValidTTL,
				CacheTTLInvalid: defaultCacheInvalidTTL,
			},
			errServerAuthEndpoint,
		},
		{
			"cacheTTL not set",
			&ServerAuth{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
				CacheTTLValid:   0,
				CacheTTLInvalid: 0,
			},
			&ServerAuth{
				ClientConfig: confighttp.ClientConfig{
					Endpoint: "http://localhost:8080",
				},
				CacheTTLValid:   defaultCacheValidTTL,
				CacheTTLInvalid: defaultCacheInvalidTTL,
			},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != tt.errExpected {
				t.Errorf("Expected error %v, got %v", tt.errExpected, err)
			}
			if tt.config.Endpoint != tt.expected.Endpoint {
				t.Errorf("Expected endpoint %s, got %s", tt.expected.Endpoint, tt.config.Endpoint)
			}
			if tt.config.CacheTTLValid != tt.expected.CacheTTLValid {
				t.Errorf("Expected CacheTTLValid %d, got %d", tt.expected.CacheTTLValid, tt.config.CacheTTLValid)
			}
			if tt.config.CacheTTLInvalid != tt.expected.CacheTTLInvalid {
				t.Errorf("Expected CacheTTLInvalid %d, got %d", tt.expected.CacheTTLInvalid, tt.config.CacheTTLInvalid)
			}
		})
	}

}
