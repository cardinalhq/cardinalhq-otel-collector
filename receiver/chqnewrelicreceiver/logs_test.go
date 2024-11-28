// Copyright 2024 CardinalHQ, Inc.
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

package newrelicreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessLogs(t *testing.T) {
	// Implement test for processing logs specific to how New Relic logs are handled
	// For example, testing JSON parsing, severity level conversion, etc.
}
func TestConvertNewRelicLogsToOTEL(t *testing.T) {
	nrLogs := []newRelicLog{
		{
			Message:   "Test log 1",
			Timestamp: 1672531200000, // Example timestamp in milliseconds
			Severity:  "INFO",
			Service:   "test-service",
		},
		{
			Message:   "Test log 2",
			Timestamp: 1672531300000,
			Severity:  "ERROR",
			Service:   "test-service",
		},
	}

	otelLogs := convertNewRelicLogsToOTEL(nrLogs)

	// Assert that the resulting Logs are not empty
	assert.NotNil(t, otelLogs)
	assert.Equal(t, 1, otelLogs.ResourceLogs().Len()) // Only 1 ResourceLogs since logs are grouped by resource

	// Validate the contents of the logs
	rl := otelLogs.ResourceLogs().At(0)
	scopeLogs := rl.ScopeLogs().At(0)
	assert.Equal(t, 2, scopeLogs.LogRecords().Len())

	// Validate the first log record
	logRecord := scopeLogs.LogRecords().At(0)
	assert.Equal(t, "Test log 1", logRecord.Body().Str())
	assert.Equal(t, "INFO", logRecord.SeverityText())
	serviceName, exists := logRecord.Attributes().Get("service.name")
	assert.True(t, exists, "Expected service.name to exist")
	assert.Equal(t, "test-service", serviceName.Str())

	// Validate the second log record
	logRecord2 := scopeLogs.LogRecords().At(1)
	assert.Equal(t, "Test log 2", logRecord2.Body().Str())
	assert.Equal(t, "ERROR", logRecord2.SeverityText())
	serviceName2, exists2 := logRecord2.Attributes().Get("service.name")
	assert.True(t, exists2, "Expected service.name to exist")
	assert.Equal(t, "test-service", serviceName2.Str())
}



func TestSplitTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     string
		expected map[string]string
	}{
		{
			"no tags",
			"",
			map[string]string{},
		},
		{
			"one tag",
			"tag1:value1",
			map[string]string{
				"tag1": "value1",
			},
		},
		{
			"multiple tags",
			"tag1:value1,tag2:value2,tag3:value3",
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		{
			"tags with spaces",
			"tag1:value1, tag2:value2,   tag3:value3",
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		// Add more tests as needed specific to your implementation
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitTags(tt.tags) // Assume splitTags is a function that splits a single string of tags into a map
			assert.Equal(t, tt.expected, result)
		})
	}
}

// You may want to add similar tests for severity conversion, etc., as shown in your Datadog example
