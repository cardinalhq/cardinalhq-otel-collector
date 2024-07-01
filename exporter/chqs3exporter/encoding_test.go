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
)

func TestGobEncoding(t *testing.T) {
	// Test that we can encode and decode a map[string]any using gob
	// This is used for encoding the tags map in the s3Exporter
	tags := map[string]any{
		"k_string":  "value1",
		"k_int8":    int8(2),
		"k_int16":   int16(3),
		"k_int32":   int32(4),
		"k_int64":   int64(123),
		"k_float64": float64(123.456),
		"k_bool":    true,
	}

	b, err := gobEncode(tags)
	assert.NoError(t, err)

	var decoded map[string]any
	err = gobDecode(b, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, tags, decoded)

	// Confirm that k_int8 and others are decoded as their original types
	assert.Equal(t, int8(2), decoded["k_int8"])
	assert.Equal(t, int16(3), decoded["k_int16"])
	assert.Equal(t, int32(4), decoded["k_int32"])
	assert.Equal(t, int64(123), decoded["k_int64"])
}
