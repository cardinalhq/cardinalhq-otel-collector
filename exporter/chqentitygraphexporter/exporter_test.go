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

package chqentitygraphexporter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestURLFor(t *testing.T) {
	tests := []struct {
		endpoint string
		ttype    string
		cid      string
		expected string
	}{
		{
			endpoint: "http://example.com",
			ttype:    "logs",
			cid:      "12345",
			expected: "http://example.com/api/v1/entityRelationships?organizationID=12345&telemetryType=logs",
		},
		{
			endpoint: "https://example.com",
			ttype:    "metrics",
			cid:      "67890",
			expected: "https://example.com/api/v1/entityRelationships?organizationID=67890&telemetryType=metrics",
		},
		{
			endpoint: "http://example.com:8080",
			ttype:    "traces",
			cid:      "C815EA44-F68E-47DA-B090-957947C0523C",
			expected: "http://example.com:8080/api/v1/entityRelationships?organizationID=c815ea44-f68e-47da-b090-957947c0523c&telemetryType=traces",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%s_%s", tt.endpoint, tt.ttype, tt.cid), func(t *testing.T) {
			result := URLFor(tt.endpoint, tt.ttype, tt.cid)
			assert.Equal(t, tt.expected, result)
		})
	}
}
