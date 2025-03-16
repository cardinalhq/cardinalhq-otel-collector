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

package converterconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLiteralMatcher(t *testing.T) {
	matcher := NewLiteralMatcher("test")

	tests := []struct {
		input    string
		expected bool
	}{
		{"test", true},
		{"Test", false},
		{"testing", false},
		{"", false},
	}

	for _, tt := range tests {
		result := matcher.Match(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestPrefixMatcher(t *testing.T) {
	matcher := NewPrefixMatcher("pre")

	tests := []struct {
		input    string
		expected bool
	}{
		{"pre", true},
		{"prefix", true},
		{"pretest", true},
		{"testpre", false},
		{"", false},
	}

	for _, tt := range tests {
		result := matcher.Match(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestSuffixMatcher(t *testing.T) {
	matcher := NewSuffixMatcher("fix")

	tests := []struct {
		input    string
		expected bool
	}{
		{"fix", true},
		{"suffix", true},
		{"testfix", true},
		{"fixtest", false},
		{"", false},
	}

	for _, tt := range tests {
		result := matcher.Match(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}
