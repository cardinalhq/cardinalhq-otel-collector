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

package datadogreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     string
		expected map[string]string
	}{
		{
			"no tags",
			"",
			map[string]string{},
		},
		{
			"one tag",
			"tag1:value1",
			map[string]string{
				"tag1": "value1",
			},
		},
		{
			"multiple tags",
			"tag1:value1,tag2:value2,tag3:value3",
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		{
			"multiple tags with spaces",
			"tag1:value1, tag2:value2,   tag3:value3",
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		{
			"tags without values",
			"tag1:,tag2:value2,tag3",
			map[string]string{
				"tag2": "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, splitTags(tt.tags))
		})
	}
}