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

func TestBufferpool_Get(t *testing.T) {
	bp := NewBufferPool("test", 10)

	// Ensure it starts empty.
	assert.Equal(t, int64(0), bp.allocations.Load())

	// Get a buffer, and ensure it allocates.
	got := bp.Get()
	assert.Equal(t, 10, cap(got.buf))
	assert.Equal(t, 10, len(got.buf))
	assert.Equal(t, int64(1), bp.allocations.Load())

	bp.Put(got)

	// Get another and check again.  Should not cause another allocation.
	got2 := bp.Get()
	assert.Equal(t, 10, cap(got2.buf))
	assert.Equal(t, 10, len(got2.buf))
	assert.Equal(t, int64(1), bp.allocations.Load())
}
