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

package chqk8smetricsconnector

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
				Events: EventsConfig{
					Interval: defaultEventsReportingInterval,
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:8080",
					},
				},
			},
			errExpected: nil,
		},
		{
			name: "invalid events reporting interval",
			config: &Config{
				Events: EventsConfig{
					Interval: 500 * time.Millisecond,
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "http://localhost:8080",
					},
				},
			},
			errExpected: errInvalidEventsReportingInterval,
		},
		{
			name: "invalid endpoint",
			config: &Config{
				Events: EventsConfig{
					Interval: defaultEventsReportingInterval,
					ClientConfig: confighttp.ClientConfig{
						Endpoint: "",
					},
				},
			},
			errExpected: errInvalidEndpoint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.errExpected != nil {
				require.ErrorIs(t, err, tt.errExpected)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
