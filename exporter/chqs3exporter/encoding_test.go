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

package chqs3exporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGobEncoding(t *testing.T) {
	// Test that we can encode and decode a map[string]any using gob
	// This is used for encoding the tags map in the s3Exporter
	tags := map[string]any{
		"k_string":          "value1",
		"k_int8":            int8(2),
		"k_int16":           int16(3),
		"k_int32":           int32(4),
		"k_int64":           int64(123),
		"k_float64":         float64(123.456),
		"k_bool":            true,
		"k_int_slice":       []int{1, 2, 3},
		"k_string_slice":    []string{"a", "b", "c"},
		"k_interface_slice": []interface{}{},
	}

	b, err := gobEncode(tags)
	require.NoError(t, err)

	var decoded map[string]any
	err = gobDecode(b, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "value1", decoded["k_string"])
	assert.Equal(t, int8(2), decoded["k_int8"])
	assert.Equal(t, int16(3), decoded["k_int16"])
	assert.Equal(t, int32(4), decoded["k_int32"])
	assert.Equal(t, int64(123), decoded["k_int64"])
	assert.Equal(t, float64(123.456), decoded["k_float64"])
	assert.Equal(t, true, decoded["k_bool"])
	assert.Equal(t, []int{1, 2, 3}, decoded["k_int_slice"])
	assert.Equal(t, []string{"a", "b", "c"}, decoded["k_string_slice"])
	assert.Equal(t, []interface{}(nil), decoded["k_interface_slice"])
}
