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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogSamplingConfig_Equals(t *testing.T) {
	tests := []struct {
		name     string
		lsc      LogSamplingConfig
		other    LogSamplingConfig
		expected bool
	}{
		{
			"equal",
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			true,
		},
		{
			"different id",
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			LogSamplingConfig{
				Id:         "id2",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			false,
		},
		{
			"different rule type",
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType2",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			false,
		},
		{
			"different scope",
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key2": "value2"},
				SampleRate: 0.5,
				RPS:        100,
			},
			false,
		},
		{
			"different sample rate",
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.6,
				RPS:        100,
			},
			false,
		},
		{
			"different rps",
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        100,
			},
			LogSamplingConfig{
				Id:         "id",
				RuleType:   "ruleType",
				Scope:      map[string]string{"key": "value"},
				SampleRate: 0.5,
				RPS:        200,
			},
			false,
		},
		{
			"empty",
			LogSamplingConfig{},
			LogSamplingConfig{},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.lsc.Equals(tt.other)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
