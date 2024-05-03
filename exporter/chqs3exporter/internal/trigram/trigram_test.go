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

func TestFieldTprint(t *testing.T) {
	tests := []struct {
		name     string
		trigram  string
		expected int64
	}{
		{
			name:     "test1",
			trigram:  "abc",
			expected: 101819565349959,
		},
		{
			name:     "test2",
			trigram:  "def",
			expected: 101819566276459,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := FieldTprint(test.name, test.trigram)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestHashcode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{
			name:     "hello",
			input:    "hello",
			expected: 99162322,
		},
		{
			name:     "world",
			input:    "world",
			expected: 113318802,
		},
		{
			name:     "foo",
			input:    "foo",
			expected: 101574,
		},
		{
			name:     "-empty-",
			input:    "",
			expected: 0,
		},
		{
			name:     "a",
			input:    "a",
			expected: 97,
		},
		{
			name:     "abc",
			input:    "abc",
			expected: 96354,
		},
		{
			name:     "xyz",
			input:    "xyz",
			expected: 119193,
		},
		{
			name:     "cardinalhq.io",
			input:    "cardinalhq.io",
			expected: 6752435815337115915,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := JavaHashcode(test.input)
			if result != test.expected {
				t.Errorf("Input: %s, Expected: %d, Got: %d", test.input, test.expected, result)
			}
		})
	}
}

func BenchmarkHashcode5len(b *testing.B) {
	s := "hello"
	for i := 0; i < b.N; i++ {
		JavaHashcode(s)
	}
}

func BenchmarkHashcode15len(b *testing.B) {
	s := "hellohellohello"
	for i := 0; i < b.N; i++ {
		JavaHashcode(s)
	}
}

func BenchmarkHashcode150len(b *testing.B) {
	s := "hellohellohello"
	for i := 0; i < 10; i++ {
		s += s
	}
	for i := 0; i < b.N; i++ {
		JavaHashcode(s)
	}
}
