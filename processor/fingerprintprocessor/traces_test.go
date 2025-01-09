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

package fingerprintprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestGetHttpResource(t *testing.T) {
	tests := []struct {
		name           string
		attributes     map[string]interface{}
		expectedResult string
	}{
		{
			name: "HTTP method and route",
			attributes: map[string]interface{}{
				httpMethod: "GET",
				httpRoute:  "/api/v1/resource",
			},
			expectedResult: "GET /api/v1/resource",
		},
		{
			name: "HTTP method and URL path",
			attributes: map[string]interface{}{
				httpMethod:  "POST",
				httpUrlPath: "/api/v1/resource/123",
			},
			expectedResult: "POST /api/v1/resource/123",
		},
		{
			name: "Only HTTP method",
			attributes: map[string]interface{}{
				httpMethod: "DELETE",
			},
			expectedResult: "DELETE",
		},
		{
			name: "Only HTTP route",
			attributes: map[string]interface{}{
				httpRoute: "/api/v1/resource",
			},
			expectedResult: "/api/v1/resource",
		},
		{
			name: "Only URL path",
			attributes: map[string]interface{}{
				httpUrlPath: "/api/v1/resource/123",
			},
			expectedResult: "/api/v1/resource/123",
		},
		{
			name:           "No attributes",
			attributes:     map[string]interface{}{},
			expectedResult: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span := ptrace.NewSpan()
			attrs := span.Attributes()
			for k, v := range tt.attributes {
				switch val := v.(type) {
				case string:
					attrs.PutStr(k, val)
				}
			}

			p := &fingerprintProcessor{
				traceFingerprinter: &mockTraceFingerprinter{},
			}
			result := p.getHttpResource(span)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

type mockTraceFingerprinter struct{}

func (m *mockTraceFingerprinter) TokenizeInput(input string) ([]string, string, error) {
	return []string{}, input, nil
}

func (m *mockTraceFingerprinter) Tokenize(input string) ([]string, string, error) {
	return []string{}, input, nil
}

func (m *mockTraceFingerprinter) Fingerprint(input string) (int64, []string, string, error) {
	return 555, nil, "foo", nil
}

func (m *mockTraceFingerprinter) IsWord(token string) bool {
	return true
}
