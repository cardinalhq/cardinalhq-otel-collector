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

	// Verify that the tags have been updated correctly
	assert.Equal(t, tags, e.tags[customerID][interval])

	// Verify that updating with different types returns an error
	err = e.updateTagMap(customerID, interval, map[string]any{
		"key1": 123,
	})
	assert.Error(t, err)
}
