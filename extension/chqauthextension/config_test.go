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
			&ServerAuth{},
			&ServerAuth{},
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
func TestSanitizeKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			"valid key",
			"abc",
			"abc",
		},
		{
			"incalid due to upppercase",
			"AbC",
			"",
		},
		{
			"invalid due to numbers",
			"abc123",
			"",
		},
		{
			"invalid key with uppercase",
			"AbC123",
			"",
		},
		{
			"key with special characters",
			"abc_123",
			"",
		},
		{
			"key with spaces",
			"abc 123",
			"",
		},
		{
			"empty key",
			"",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeKey(tt.key)
			if result != tt.expected {
				t.Errorf("Expected sanitized key %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestSanitizeValue(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			"valid value",
			"abc",
			"abc",
		},
		{
			"value with symbols",
			"abc_-./@:,123",
			"abc_-./@:,123",
		},
		{
			"valid value with spaces",
			"abc 123",
			"",
		},
		{
			"invalid value with non-printable characters",
			"abc\x00",
			"",
		},
		{
			"empty value",
			"",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeValue(tt.value)
			if result != tt.expected {
				t.Errorf("Expected sanitized value %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expected    *Config
		errExpected error
	}{
		{
			"valid client auth",
			&Config{
				ClientAuth: &ClientAuth{
					APIKey: "key",
					Environment: map[string]string{
						"markus": "value1",
					},
				},
			},
			&Config{
				ClientAuth: &ClientAuth{
					APIKey: "key",
					Environment: map[string]string{
						"markus": "value1",
					},
				},
			},
			nil,
		},
		{
			"valid server auth",
			&Config{
				ServerAuth: &ServerAuth{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:8080",
					},
				},
			},
			&Config{
				ServerAuth: &ServerAuth{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:8080",
					},
					CacheTTLValid:   defaultCacheValidTTL,
					CacheTTLInvalid: defaultCacheInvalidTTL,
				},
			},
			nil,
		},
		{
			"no auth config",
			&Config{},
			&Config{},
			errNoAuthConfig,
		},
		{
			"both auth config",
			&Config{
				ClientAuth: &ClientAuth{
					APIKey: "key",
				},
				ServerAuth: &ServerAuth{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:8080",
					},
				},
			},
			&Config{
				ClientAuth: &ClientAuth{
					APIKey: "key",
				},
				ServerAuth: &ServerAuth{
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:8080",
					},
				},
			},
			errDuplicateAuthConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != tt.errExpected {
				t.Errorf("Expected error %v, got %v", tt.errExpected, err)
			}
			if tt.config.ClientAuth != nil {
				if tt.config.ClientAuth.APIKey != tt.expected.ClientAuth.APIKey {
					t.Errorf("Expected APIKey %s, got %s", tt.expected.ClientAuth.APIKey, tt.config.ClientAuth.APIKey)
				}
				if len(tt.config.ClientAuth.Environment) != len(tt.expected.ClientAuth.Environment) {
					t.Errorf("Expected Environment %v, got %v", tt.expected.ClientAuth.Environment, tt.config.ClientAuth.Environment)
				}
			}
			if tt.config.ServerAuth != nil {
				if tt.config.ServerAuth.Endpoint != tt.expected.ServerAuth.Endpoint {
					t.Errorf("Expected Endpoint %s, got %s", tt.expected.ServerAuth.Endpoint, tt.config.ServerAuth.Endpoint)
				}
				if tt.config.ServerAuth.CacheTTLValid != tt.expected.ServerAuth.CacheTTLValid {
					t.Errorf("Expected CacheTTLValid %d, got %d", tt.expected.ServerAuth.CacheTTLValid, tt.config.ServerAuth.CacheTTLValid)
				}
				if tt.config.ServerAuth.CacheTTLInvalid != tt.expected.ServerAuth.CacheTTLInvalid {
					t.Errorf("Expected CacheTTLInvalid %d, got %d", tt.expected.ServerAuth.CacheTTLInvalid, tt.config.ServerAuth.CacheTTLInvalid)
				}
			}
		})
	}
}

func TestClientAuth_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *ClientAuth
		expected    *ClientAuth
		errExpected error
	}{
		{
			"valid",
			&ClientAuth{
				APIKey: "key",
				Environment: map[string]string{
					"markus": "value1",
				},
			},
			&ClientAuth{
				APIKey: "key",
				Environment: map[string]string{
					"markus": "value1",
				},
			},
			nil,
		},
		{
			"no API key",
			&ClientAuth{
				APIKey: "",
			},
			&ClientAuth{
				APIKey: "",
			},
			errNoClientAPIKey,
		},
		{
			"invalid environment key",
			&ClientAuth{
				APIKey: "key",
				Environment: map[string]string{
					"markus1": "value1",
				},
			},
			&ClientAuth{
				APIKey: "key",
				Environment: map[string]string{
					"markus1": "value1",
				},
			},
			errBadEnvironemntKey,
		},
		{
			"invalid environment value",
			&ClientAuth{
				APIKey: "key",
				Environment: map[string]string{
					"markus": "value\x00",
				},
			},
			&ClientAuth{
				APIKey: "key",
				Environment: map[string]string{
					"markus": "value\x00",
				},
			},
			errBadEnvironmentValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != tt.errExpected {
				t.Errorf("Expected error %v, got %v", tt.errExpected, err)
			}
			if tt.config.APIKey != tt.expected.APIKey {
				t.Errorf("Expected APIKey %s, got %s", tt.expected.APIKey, tt.config.APIKey)
			}
			if len(tt.config.Environment) != len(tt.expected.Environment) {
				t.Errorf("Expected Environment %v, got %v", tt.expected.Environment, tt.config.Environment)
			}
		})
	}
}
