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

	"github.com/honeycombio/dynsampler-go"
	"github.com/stretchr/testify/assert"
)

func TestSamplerForType(t *testing.T) {
	cases := []struct {
		name         string
		config       LogSamplingConfig
		expectedType LogRuleType
		expected     dynsampler.Sampler
	}{
		{
			name: "random rule type",
			config: LogSamplingConfig{
				RuleType:   "random",
				SampleRate: 0.5,
			},
			expectedType: LogRuleTypeRandom,
			expected:     &dynsampler.Static{Default: 2},
		},
		{
			name: "rps rule type",
			config: LogSamplingConfig{
				RuleType: "rps",
				RPS:      100,
			},
			expectedType: LogRuleTypeRPS,
			expected:     &dynsampler.AvgSampleWithMin{GoalSampleRate: 1, MinEventsPerSec: 100},
		},
		{
			name: "unknown rule type",
			config: LogSamplingConfig{
				RuleType: "unknown",
			},
			expectedType: LogRuleTypeUnknown,
			expected:     nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actualType, actualSampler := samplerForType(tc.config)
			assert.Equal(t, tc.expectedType, actualType)
			assert.Equal(t, tc.expected, actualSampler)
		})
	}
}

func TestSamplerForCorrectness(t *testing.T) {
	tests := []struct {
		name             string
		ruleConfig       LogSamplingConfig
		minExpectedTrues int
		maxExpectedTrues int
		runs             int
	}{
		{
			"rps == 0",
			LogSamplingConfig{
				RuleType: "rps",
				RPS:      0,
			},
			100_000, 100_000,
			100_000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, sampler := samplerForType(tt.ruleConfig)
			assert.NotNil(t, sampler)
			err := sampler.Start()
			assert.NoError(t, err)
			trueCount := 0
			for i := 0; i < tt.runs; i++ {
				rate := sampler.GetSampleRate("test")
				matched := shouldFilter(float64(rate), rand.Float64())
				if matched {
					trueCount++
				}
			}
			err = sampler.Stop()
			assert.NoError(t, err)
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
		{
			name:     "rate is 1",
			rate:     1,
			expected: 1.0,
		},
		{
			name:     "rate is 2",
			rate:     2,
			expected: 0.5,
		},
		{
			name:     "rate is 5",
			rate:     5,
			expected: 0.2,
		},
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
		{
			name:     "rate is 0.5",
			rate:     0.5,
			expected: 2,
		},
		{
			name:     "rate is 0.2",
			rate:     0.2,
			expected: 5,
		},
		{
			name:     "rate is 0.1",
			rate:     0.1,
			expected: 10,
		},
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
		rate     float64
		randval  float64
		expected bool
	}{
		{
			name:     "rate is 0, randval is 0",
			rate:     0,
			randval:  0,
			expected: true,
		},
		{
			name:     "rate is 0, randval is 0.5",
			rate:     0,
			randval:  0.5,
			expected: true,
		},
		{
			name:     "rate is 0, randval is 1",
			rate:     0,
			randval:  1,
			expected: true,
		},
		{
			name:     "rate is 1, randval is 0",
			rate:     1,
			randval:  0,
			expected: false,
		},
		{
			name:     "rate is 1, randval is 1",
			rate:     1,
			randval:  1,
			expected: false,
		},
		{
			name:     "rate is 2, randval is 0.25",
			rate:     2,
			randval:  0.25,
			expected: false,
		},
		{
			name:     "rate is 2, randval is 0.75",
			rate:     2,
			randval:  0.75,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := shouldFilter(tt.rate, tt.randval)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
