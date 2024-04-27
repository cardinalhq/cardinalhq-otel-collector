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

package accumulator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAccumulatorImpl(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	accumulator := NewAccumulatorImlp(buckets)

	assert.NotNil(t, accumulator)
	assert.Equal(t, buckets, accumulator.buckets)
	assert.Equal(t, uint64(0), accumulator.count)
	assert.Equal(t, make([]int64, len(buckets)), accumulator.sum)
}

func TestAccumulatorImpl_Add(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	accumulator := NewAccumulatorImlp(buckets)

	// Test case 1: Add valid value
	value := []int64{10, 20, 30, 40, 50}
	err := accumulator.Add(value)
	assert.NoError(t, err)
	err = accumulator.Add([]int64{1, 2, 3, 4, 5})
	assert.NoError(t, err)
	assert.Equal(t, []int64{11, 22, 33, 44, 55}, accumulator.sum)
	assert.Equal(t, uint64(2), accumulator.count)

	// Test case 2: Add value with incorrect length
	value = []int64{10, 20, 30, 40}
	err = accumulator.Add(value)
	assert.Error(t, err)
	assert.Equal(t, []int64{11, 22, 33, 44, 55}, accumulator.sum)
	assert.Equal(t, uint64(2), accumulator.count)
}

func TestAccumulatorImpl_Buckets(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	accumulator := NewAccumulatorImlp(buckets)

	assert.Equal(t, buckets, accumulator.Buckets())
}

func TestAccumulatorImpl_Count(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	accumulator := NewAccumulatorImlp(buckets)

	assert.Equal(t, uint64(0), accumulator.Count())

	accumulator.count = 10
	assert.Equal(t, uint64(10), accumulator.Count())
}

func TestAccumulatorImpl_Sum(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	accumulator := NewAccumulatorImlp(buckets)

	assert.Equal(t, make([]int64, len(buckets)), accumulator.Sum())

	accumulator.sum = []int64{10, 20, 30, 40, 50}
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, accumulator.Sum())
}

func TestAccumulatorImpl_Avg(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	accumulator := NewAccumulatorImlp(buckets)

	// Test case 1: Empty accumulator
	assert.Equal(t, make([]int64, len(buckets)), accumulator.Avg())

	// Test case 2: Non-empty accumulator
	accumulator.sum = []int64{10, 20, 30, 40, 50}
	accumulator.count = 5
	assert.Equal(t, []int64{2, 4, 6, 8, 10}, accumulator.Avg())
}
