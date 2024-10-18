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
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestMatchTag(t *testing.T) {
	itemAttrs := pcommon.NewMap()
	itemAttrs.PutStr("key1", "value1")
	itemAttrs.PutStr("key2", "value2")
	itemAttrs.PutInt("code", 200)

	type test struct {
		name         string
		lookupKey    string
		compareValue string
		expected     bool
	}

	tests := []test{
		{
			name:         "Matching tag exists",
			lookupKey:    "key1",
			compareValue: "value1",
			expected:     true,
		},
		{
			name:         "Matching tag does not exist",
			lookupKey:    "key3",
			compareValue: "value3",
			expected:     false,
		},
		{
			name:         "Non-matching tag exists",
			lookupKey:    "key2",
			compareValue: "value1",
			expected:     false,
		},
		{
			name:         "integer compare works",
			lookupKey:    "code",
			compareValue: "200",
			expected:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := matchTag(tc.lookupKey, itemAttrs, tc.compareValue)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestMatchScope(t *testing.T) {
	attr1 := pcommon.NewMap()
	attr1.PutStr("service.name", "value1")

	attr2 := pcommon.NewMap()
	attr2.PutStr("key2", "value2")

	attr3 := pcommon.NewMap()
	attr3.PutStr("key3", "value3")

	attrs := map[string]pcommon.Map{
		"resource": attr1,
		"scope":    attr2,
		"log":      attr3,
	}

	tests := []struct {
		name     string
		scope    map[string]string
		expected bool
	}{
		{
			name:     "Empty scope",
			scope:    map[string]string{},
			expected: true,
		},
		{
			name:     "Matching scope",
			scope:    map[string]string{"resource.service.name": "value1"},
			expected: true,
		},
		{
			name:     "Non-matching scope",
			scope:    map[string]string{"resource.service.name": "value2"},
			expected: false,
		},
		{
			name:     "Non-matching scope key",
			scope:    map[string]string{"attr4.service.name": "value1"},
			expected: false,
		},
		{
			name:     "incorrect scope key",
			scope:    map[string]string{"resource": "value1"},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := matchscope(tc.scope, attrs)
			assert.Equal(t, tc.expected, actual)
		})
	}

}
