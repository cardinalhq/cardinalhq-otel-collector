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

package sampler

import (
	"math/rand"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSamplerForType(t *testing.T) {
	cases := []struct {
		name         string
		config       EventSamplingConfigV1
		expectedType EventSamplingRuleType
		expected     Sampler
	}{
		{
			name: "random rule type",
			config: EventSamplingConfigV1{
				RuleType:   "random",
				SampleRate: 0.5,
			},
			expectedType: EventSamplingRuleTypeRandom,
			expected:     &StaticSampler{fixedRate: 2},
		},
		{
			name: "rps rule type",
			config: EventSamplingConfigV1{
				RuleType: "rps",
				RPS:      100,
			},
			expectedType: EventSamplingRuleTypeRPS,
			expected:     &rpsSampler{MaxRPS: 100, clearFrequencyDuration: defaultClearFrequencyDuration},
		},
		{
			name: "unknown rule type",
			config: EventSamplingConfigV1{
				RuleType: "unknown",
			},
			expectedType: EventSamplingRuleTypeUnknown,
			expected:     nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actualType, actualSampler := samplerForType(tc.config, nil)
			assert.Equal(t, tc.expectedType, actualType)
			assert.Equal(t, tc.expected, actualSampler)
		})
	}
}

func TestShouldFilterRates(t *testing.T) {
	tests := []struct {
		name             string
		rate             int
		minExpectedTrues int
		maxExpectedTrues int
		runs             int
	}{
		{"rps == 0", 0, 100_000, 100_000, 100_000},
		{"rps == 1", 1, 0, 0, 100_000},
		{"rps == 2", 2, 40_000, 60_000, 100_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trueCount := 0
			for i := 0; i < tt.runs; i++ {
				matched := shouldFilter(tt.rate, rand.Float64())
				if matched {
					trueCount++
				}
			}
			assert.True(t, trueCount >= tt.minExpectedTrues, "expected at least %d trues, got %d", tt.minExpectedTrues, trueCount)
			assert.True(t, trueCount <= tt.maxExpectedTrues, "expected at most %d trues, got %d", tt.maxExpectedTrues, trueCount)
		})
	}
}

func TestRPSToRandom(t *testing.T) {
	tests := []struct {
		name     string
		rate     int
		expected float64
	}{
		{"rate is 1", 1, 1.0},
		{"rate is 2", 2, 0.5},
		{"rate is 5", 5, 0.2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := rpsToRandom(tt.rate)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestRandomToRPS(t *testing.T) {
	tests := []struct {
		name     string
		rate     float64
		expected int
	}{
		{"rate is 0.5", 0.5, 2},
		{"rate is 0.2", 0.2, 5},
		{"rate is 0.1", 0.1, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := randomToRPS(tt.rate)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestShouldFilter(t *testing.T) {
	tests := []struct {
		name     string
		rate     int
		randval  float64
		expected bool
	}{
		{"rate is 0, randval is 0", 0, 0, true},
		{"rate is 0, randval is 0.5", 0, 0.5, true},
		{"rate is 0, randval is 1", 0, 1, true},
		{"rate is 1, randval is 0", 1, 0, false},
		{"rate is 1, randval is 1", 1, 1, false},
		{"rate is 2, randval is 0.25", 2, 0.25, false},
		{"rate is 2, randval is 0.75", 2, 0.75, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := shouldFilter(tt.rate, tt.randval)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
func TestGetServiceName(t *testing.T) {
	tests := []struct {
		name     string
		attrmap  map[string]string
		expected string
	}{
		{
			"service name exists",
			map[string]string{"service.name": "my-service"},
			"my-service",
		},
		{
			"service name does not exist",
			map[string]string{},
			"unknown-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rattr := pcommon.NewMap()
			for k, v := range tt.attrmap {
				rattr.PutStr(k, v)
			}
			actual := getServiceName(rattr)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func getServiceName(rattr pcommon.Map) string {
	serviceName, ok := rattr.Get("service.name")
	if !ok {
		return "unknown-service"
	}
	return serviceName.AsString()
}

func makeConstantSampler() Sampler {
	return &StaticSampler{fixedRate: 0}
}

func TestShouldFilterLog(t *testing.T) {
	rule1, err := newFilterRule(EventSamplingConfigV1{
		Filter: []Filter{
			{ContextId: "resource", Condition: `attributes["service.name"] == "other-service"`},
		},
	},
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	assert.NoError(t, err)

	matchingRule, err := newFilterRule(EventSamplingConfigV1{
		Id:         "matchingRule",
		RuleType:   "rps",
		SampleRate: 0.5,
		RPS:        100,
		Vendor:     "test-vendor",
		Filter: []Filter{
			{ContextId: "resource", Condition: `attributes["service.name"] == "my-service"`},
		},
	},
		component.TelemetrySettings{Logger: zap.NewNop()})
	matchingRule.sampler = makeConstantSampler()

	assert.NoError(t, err)

	tests := []struct {
		name     string
		rules    map[string]*filterRule
		rattr    map[string]string
		iattr    map[string]string
		lattr    map[string]string
		expected []string
	}{
		{
			"no rules",
			map[string]*filterRule{},
			map[string]string{"service.name": "my-service"},
			map[string]string{},
			map[string]string{},
			[]string{""},
		},
		{
			"no matching rules",
			map[string]*filterRule{
				"rule1": rule1,
			},
			map[string]string{"service.name": "my-service"},
			map[string]string{},
			map[string]string{},
			[]string{""},
		},
		{
			"matching rule",
			map[string]*filterRule{
				"matchingRule": matchingRule,
			},
			map[string]string{"service.name": "my-service"},
			map[string]string{},
			map[string]string{},
			[]string{"matchingRule"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock data for ResourceLogs, ScopeLogs, and LogRecord
			rl := plog.NewResourceLogs()
			rattr := rl.Resource().Attributes()
			for k, v := range tt.rattr {
				rattr.PutStr(k, v)
			}

			sl := plog.NewScopeLogs()
			iattr := sl.Scope().Attributes()
			for k, v := range tt.iattr {
				iattr.PutStr(k, v)
			}

			ll := plog.NewLogRecord()
			lattr := ll.Attributes()
			for k, v := range tt.lattr {
				lattr.PutStr(k, v)
			}

			// Initialize the EventSamplerImpl with the test rules
			ls := &EventSamplerImpl{
				rules:  tt.rules,
				logger: zap.NewNop(), // Use a no-op logger for testing
			}

			// Call shouldFilterLog to get the result
			actual := ls.shouldFilterLog("", "", rl, sl, ll)

			// Check if the actual result matches any of the expected rules
			assert.Contains(t, tt.expected, actual)
		})
	}
}