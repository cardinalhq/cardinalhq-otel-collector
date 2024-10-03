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
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Helper function to create a test EventSamplingConfigV1
func createTestConfig(contextId string, condition string) EventSamplingConfigV1 {
	return EventSamplingConfigV1{
		Id:         "test-id",
		RuleType:   "random",
		Filter:     []Filter{{ContextId: contextId, Condition: condition}},
		SampleRate: 0.5,
		Vendor:     "test-vendor",
	}
}

// Logs Helper function to create a ResourceLogs with a mock resource
func createTestResourceLogs() plog.ResourceLogs {
	rl := plog.NewResourceLogs()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	return rl
}

// Logs Helper function to create a ScopeLogs with a mock scope
func createTestScopeLogs() plog.ScopeLogs {
	sl := plog.NewScopeLogs()
	sl.Scope().SetName("test-scope")
	return sl
}

// Logs Helper function to create a LogRecord with mock log attributes
func createTestLogRecord() plog.LogRecord {
	ll := plog.NewLogRecord()
	ll.Attributes().PutStr("log.level", "INFO")
	return ll
}

// Spans Helper function to create a ResourceSpans with a mock resource
func createTestResourceSpans() ptrace.ResourceSpans {
	rs := ptrace.NewResourceSpans()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	return rs
}

// Spans Helper function to create a ScopeSpans with a mock scope
func createTestScopeSpans() ptrace.ScopeSpans {
	ss := ptrace.NewScopeSpans()
	ss.Scope().SetName("test-scope")
	return ss
}

// Spans Helper function to create a Span with mock span attributes
func createTestSpan() ptrace.Span {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("span.kind", "internal")
	return span
}

// LOGS TESTS

// TestFilterRule_ResourceConditionLog Test resource-based condition for logs
func TestFilterRule_ResourceConditionLog(t *testing.T) {
	// Create a filterRule with a resource-based condition
	config := createTestConfig("resource", `attributes["service.name"] == "test-service"`)
	rule := newFilterRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	// Evaluate the filter rule for logs
	result := rule.evaluateLog(rl, sl, ll)
	assert.True(t, result, "Resource-based condition for logs should evaluate to true")
}

// TestFilterRule_ScopeConditionLog Test scope-based condition for logs
func TestFilterRule_ScopeConditionLog(t *testing.T) {
	// Create a filterRule with a scope-based condition
	config := createTestConfig("scope", `scope.name == "test-scope"`)
	rule := newFilterRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	// Evaluate the filter rule for logs
	result := rule.evaluateLog(rl, sl, ll)
	assert.True(t, result, "Scope-based condition for logs should evaluate to true")
}

// TestFilterRule_LogRecordConditionLog Test log record-based condition for logs
func TestFilterRule_LogRecordConditionLog(t *testing.T) {
	// Create a filterRule with a log record-based condition
	config := createTestConfig("log", `attributes["log.level"] == "INFO"`)
	rule := newFilterRule(config)

	// Create test data
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()
	ll := createTestLogRecord()

	// Evaluate the filter rule for logs
	result := rule.evaluateLog(rl, sl, ll)
	assert.True(t, result, "Log record-based condition should evaluate to true")
}

// TestFilterRule_LogRecordConditionLog_Negative Test log record-based condition with mismatched log data
func TestFilterRule_LogRecordConditionLog_Negative(t *testing.T) {
	// Create a filterRule with a log record-based condition
	config := createTestConfig("log", `attributes["log.level"] == "INFO"`)
	rule := newFilterRule(config)

	// Create test data with mismatched log level
	rl := createTestResourceLogs()
	sl := createTestScopeLogs()

	// LogRecord with different log level
	ll := plog.NewLogRecord()
	ll.Attributes().PutStr("log.level", "DEBUG") // Mismatched log level

	// Evaluate the filter rule for logs
	result := rule.evaluateLog(rl, sl, ll)
	assert.False(t, result, "Log record-based condition should evaluate to false when log level does not match")
}

// SPANS TESTS

// TestFilterRule_ResourceConditionSpan Test resource-based condition for spans
func TestFilterRule_ResourceConditionSpan(t *testing.T) {
	// Create a filterRule with a resource-based condition
	config := createTestConfig("resource", `attributes["service.name"] == "test-service"`)
	rule := newFilterRule(config)

	// Create test data
	rs := createTestResourceSpans()
	ss := createTestScopeSpans()
	span := createTestSpan()

	// Evaluate the filter rule for spans
	result := rule.evaluateSpan(rs, ss, span)
	assert.True(t, result, "Resource-based condition for span should evaluate to true")
}

// TestFilterRule_ScopeConditionSpan Test scope-based condition for spans
func TestFilterRule_ScopeConditionSpan(t *testing.T) {
	// Create a filterRule with a scope-based condition
	config := createTestConfig("scope", `scope.name == "test-scope"`)
	rule := newFilterRule(config)

	// Create test data
	rs := createTestResourceSpans()
	ss := createTestScopeSpans()
	span := createTestSpan()

	// Evaluate the filter rule for spans
	result := rule.evaluateSpan(rs, ss, span)
	assert.True(t, result, "Scope-based condition for span should evaluate to true")
}

// TestFilterRule_SpanConditionSpan Test span-based condition for spans
func TestFilterRule_SpanConditionSpan(t *testing.T) {
	// Create a filterRule with a span-based condition
	config := createTestConfig("span", `attributes["span.kind"] == "internal"`)
	rule := newFilterRule(config)

	// Create test data
	rs := createTestResourceSpans()
	ss := createTestScopeSpans()
	span := createTestSpan()

	// Evaluate the filter rule for spans
	result := rule.evaluateSpan(rs, ss, span)
	assert.True(t, result, "Span-based condition should evaluate to true")
}

// TestFilterRule_SpanConditionSpan_Negative Test span-based condition with mismatched span data
func TestFilterRule_SpanConditionSpan_Negative(t *testing.T) {
	// Create a filterRule with a span-based condition
	config := createTestConfig("span", `attributes["span.kind"] == "internal"`)
	rule := newFilterRule(config)

	// Create test data with mismatched span kind
	rs := createTestResourceSpans()
	ss := createTestScopeSpans()
	span := ptrace.NewSpan()
	span.Attributes().PutStr("span.kind", "external") // Mismatched span kind

	// Evaluate the filter rule for spans
	result := rule.evaluateSpan(rs, ss, span)
	assert.False(t, result, "Span-based condition should evaluate to false when span.kind does not match")
}
