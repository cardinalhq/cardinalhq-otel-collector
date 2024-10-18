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

package ottl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRuletypeToInt(t *testing.T) {
	testCases := []struct {
		name     string
		ruleType string
		expected EventSamplingRuleType
	}{
		{
			name:     "random rule type",
			ruleType: "random",
			expected: EventSamplingRuleTypeRandom,
		},
		{
			name:     "rps rule type",
			ruleType: "rps",
			expected: EventSamplingRuleTypeRPS,
		},
		{
			name:     "unknown rule type",
			ruleType: "badruletype",
			expected: EventSamplingRuleTypeUnknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := samplingRuleTypeToInt(tc.ruleType)
			assert.Equal(t, tc.expected, got)
		})
	}
}
