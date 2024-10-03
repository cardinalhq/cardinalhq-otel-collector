// Package sampler Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package sampler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Helper function to create a test LogSamplingConfigV1
func createTestConfig(contextId string, condition string) LogSamplingConfigV1 {
	return LogSamplingConfigV1{
		Id:         "test-id",
		RuleType:   "random",
		Filter:     []Filter{{ContextId: contextId, Condition: condition}},
		SampleRate: 0.5,
		Vendor:     "test-vendor",
	}
}

// Helper function to create a ResourceLogs with a mock resource
func createTestResourceLogs() plog.ResourceLogs {
	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	return rl
}

// Helper function to create a ScopeLogs with a mock scope
func createTestScopeLogs() plog.ScopeLogs {
	sl := plog.NewScopeLogs()
	sl.Scope().SetName("test-scope")
	return sl
}

// Helper function to create a LogRecord with mock log attributes
func createTestLogRecord() plog.LogRecord {
	ll := plog.NewLogRecord()
	ll.Attributes().PutStr("log.level", "INFO")
	return ll
}

// TestLogRule_ResourceCondition Test resource-based condition
func TestLogRule_ResourceCondition(t *testing.T) {
	// Create a logRule with a resource-based condition
	config := createTestConfig("resource", `attributes["service.name"] == "test-service"`)
	rule := newLogRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	result := rule.evaluate(rl, sl, ll)
	assert.True(t, result, "Resource-based condition should evaluate to true")
}

// TestLogRule_ScopeCondition Test scope-based condition
func TestLogRule_ScopeCondition(t *testing.T) {
	// Create a logRule with a scope-based condition
	config := createTestConfig("scope", `scope.name == "test-scope"`)
	rule := newLogRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	result := rule.evaluate(rl, sl, ll)
	assert.True(t, result, "Scope-based condition should evaluate to true")
}

// TestLogRule_LogRecordCondition Test log record-based condition
func TestLogRule_LogRecordCondition(t *testing.T) {
	// Create a logRule with a log record-based condition
	config := createTestConfig("log", `attributes["log.level"] == "INFO"`)
	rule := newLogRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	result := rule.evaluate(rl, sl, ll)
	assert.True(t, result, "Log record-based condition should evaluate to true")
}

// TestLogRule_LogRecordCondition_Negative checks that log record-based conditions evaluate to false when they do not match
func TestLogRule_LogRecordCondition_Negative(t *testing.T) {
	// Create a logRule with a log record-based condition that expects log level "INFO"
	config := createTestConfig("log", `attributes["log.level"] == "INFO"`)
	rule := newLogRule(config)

	// Create test data with a log record that does not match the condition
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()

	// Create a LogRecord with a different log level, which should cause the condition to evaluate to false
	ll := plog.NewLogRecord()
	ll.Attributes().PutStr("log.level", "DEBUG") // Different log level than the condition

	// Evaluate the log rule
	result := rule.evaluate(rl, sl, ll)

	// Assert that the condition evaluates to false
	assert.False(t, result, "Log record-based condition should evaluate to false when log level does not match")
}

// TestLogRule_AllConditions Test all three conditions (resource, scope, and log)
func TestLogRule_AllConditions(t *testing.T) {
	// Create a logRule with conditions for all three contexts
	config := LogSamplingConfigV1{
		Id:       "test-id",
		RuleType: "random",
		Filter: []Filter{
			{ContextId: "resource", Condition: `attributes["service.name"] == "test-service"`},
			{ContextId: "scope", Condition: `scope.name == "test-scope"`},
			{ContextId: "log", Condition: `attributes["log.level"] == "INFO"`},
		},
		SampleRate: 0.5,
		Vendor:     "test-vendor",
	}
	rule := newLogRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	result := rule.evaluate(rl, sl, ll)
	assert.True(t, result, "All conditions should evaluate to true")
}
