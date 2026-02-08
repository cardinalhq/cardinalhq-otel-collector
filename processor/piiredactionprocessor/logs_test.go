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

package piiredactionprocessor

import (
	"testing"

	"github.com/cardinalhq/oteltools/pkg/pii"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.uber.org/zap"
)

func TestGetServiceName(t *testing.T) {
	attr := pcommon.NewMap()
	attr.PutStr(string(semconv.ServiceNameKey), "my-service")
	serviceName := getServiceName(attr)
	assert.Equal(t, "my-service", serviceName)

	attr = pcommon.NewMap()
	serviceName = getServiceName(attr)
	assert.Equal(t, "unknown", serviceName)
}

func TestPiiRedactionProcessor_SanitizeBodyString(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		sanitized string
	}{
		{
			name:      "sanitize successful",
			input:     "4111-1111-1111-1111",
			sanitized: "REDACTED",
		},
	}

	detector := pii.NewDetector()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			processor := &piiRedactionProcessor{
				detector: detector,
				logger:   logger,
			}

			logRecord := plog.NewLogRecord()
			logRecord.Body().SetStr(tt.input)

			processor.sanitizeBodyString(logRecord)

			assert.Equal(t, tt.sanitized, logRecord.Body().AsString())
		})
	}
}

func TestPiiRedactionProcessor_SanitizeBodyMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected map[string]any
	}{
		{
			name: "sanitize string values",
			input: map[string]any{
				"totally-a-ccn": "the card is 4111-1111-1111-1111",
				"key2":          "nothing here to hide",
			},
			expected: map[string]any{
				"totally-a-ccn": "the card is REDACTED",
				"key2":          "nothing here to hide",
			},
		},
		{
			name: "sanitize only top level strings",
			input: map[string]any{
				"totally-a-ccn": "the card is 4111-1111-1111-1111",
				"nested": map[string]any{
					"ssn": "123-45-6789",
				},
			},
			expected: map[string]any{
				"totally-a-ccn": "the card is REDACTED",
				"nested": map[string]any{
					"ssn": "123-45-6789",
				},
			},
		},
		{
			name: "ignore non-string values",
			input: map[string]any{
				"totally-a-ccn": 4111111111111111,
				"key2":          "nothing here to hide",
			},
			expected: map[string]any{
				"totally-a-ccn": int64(4111111111111111),
				"key2":          "nothing here to hide",
			},
		},
	}

	detector := pii.NewDetector()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logRecord := plog.NewLogRecord()
			err := logRecord.Body().SetEmptyMap().FromRaw(tt.input)
			assert.NoError(t, err)

			logger := zap.NewNop()
			processor := &piiRedactionProcessor{
				detector: detector,
				logger:   logger,
			}

			processor.sanitizeBodyMap(logRecord)

			result := logRecord.Body().AsRaw()
			assert.Equal(t, tt.expected, result)
		})
	}
}
