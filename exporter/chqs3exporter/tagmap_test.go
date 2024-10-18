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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateTagMap(t *testing.T) {
	e := &s3Exporter{
		tags:    make(map[string]map[int64]map[string]any),
		taglock: sync.Mutex{},
	}

	customerID := "12345"
	interval := int64(60)
	tags := map[string]any{
		"key1": "value1",
		"key2": "value2",
	}

	err := e.updateTagMap(customerID, interval, tags)
	assert.NoError(t, err)
	assert.Equal(t, tags, e.tags[customerID][interval])

	// Verify that updating with different types returns an error
	err = e.updateTagMap(customerID, interval, map[string]any{
		"key1": 123,
	})
	assert.Error(t, err)

	// Add another field to the map
	err = e.updateTagMap(customerID, interval, map[string]any{
		"key3": 1234,
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(e.tags[customerID][interval]))
	newmap := map[string]any{
		"key1": "value1",
		"key2": "value2",
		"key3": 1234,
	}
	assert.Equal(t, newmap, e.tags[customerID][interval])

	// add a list to the map
	err = e.updateTagMap(customerID, interval, map[string]any{
		"key4": []int{1, 2, 3},
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(e.tags[customerID][interval]))
	newmap["key4"] = `[1,2,3]`
	assert.Equal(t, newmap, e.tags[customerID][interval])
}

func TestHandleValue(t *testing.T) {
	tests := []struct {
		input    any
		expected any
	}{
		{"string", "string"},
		{123, 123},
		{int8(8), int8(8)},
		{int16(16), int16(16)},
		{int32(32), int32(32)},
		{int64(64), int64(64)},
		{uint(123), uint(123)},
		{uint8(8), uint8(8)},
		{uint16(16), uint16(16)},
		{uint32(32), uint32(32)},
		{uint64(64), uint64(64)},
		{float32(1.23), float32(1.23)},
		{float64(1.23), float64(1.23)},
		{true, true},
		{false, false},
		{[]int{1, 2, 3}, `[1,2,3]`},
		{map[string]any{"key": "value"}, `{"key":"value"}`},
	}

	for _, test := range tests {
		result := handleValue(test.input)
		assert.Equal(t, test.expected, result)
	}
}
