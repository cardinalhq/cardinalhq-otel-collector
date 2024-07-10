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

package idgen

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestXidGenerator_Make(t *testing.T) {
	x := NewXIDGenerator()
	id := x.Make(time.Now())
	assert.NotEmpty(t, id, "failed to generate ID")
	assert.Len(t, id, 20, "ID has wrong length")
}

func TestXidGenerator_Make_many(t *testing.T) {
	x := NewXIDGenerator()
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := x.Make(time.Now())
		ids[id] = true
	}
	assert.Len(t, ids, 1000, "failed to generate unique IDs")
	for id := range ids {
		assert.Len(t, id, 20, "ID %s has wrong length", id)
	}
}
