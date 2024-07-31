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

package trigram

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToTrigrams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "hello",
			input:    "hello",
			expected: []string{"hel", "ell", "llo", FieldExistsMarker},
		},
		{
			name:     "world",
			input:    "world",
			expected: []string{"wor", "orl", "rld", FieldExistsMarker},
		},
		{
			name:     "foo",
			input:    "foo",
			expected: []string{"foo", FieldExistsMarker},
		},
		{
			name:     "-empty-",
			input:    "",
			expected: []string{FieldExistsMarker},
		},
		{
			name:     "a",
			input:    "a",
			expected: []string{FieldExistsMarker},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ToTrigrams(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}
