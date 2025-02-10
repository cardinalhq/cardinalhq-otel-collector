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

package fingerprintprocessor

import (
	"testing"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/stretchr/testify/assert"
)

func TestMakeFingerprintMapForConfig(t *testing.T) {
	tests := []struct {
		name     string
		input    []ottl.FingerprintMapping
		expected map[int64]int64
	}{
		{
			name: "single mapping",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{2, 3},
				},
			},
			expected: map[int64]int64{
				2: 1,
				3: 1,
			},
		},
		{
			name: "multiple mappings",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{2, 3},
				},
				{
					Primary: 4,
					Aliases: []int64{5, 6},
				},
			},
			expected: map[int64]int64{
				2: 1,
				3: 1,
				5: 4,
				6: 4,
			},
		},
		{
			name:     "empty input",
			input:    []ottl.FingerprintMapping{},
			expected: map[int64]int64{},
		},
		{
			name: "no aliases",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: []int64{},
				},
			},
			expected: map[int64]int64{},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: map[int64]int64{},
		},
		{
			name: "nil aliases",
			input: []ottl.FingerprintMapping{
				{
					Primary: 1,
					Aliases: nil,
				},
			},
			expected: map[int64]int64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := makeFingerprintMapForConfig(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
