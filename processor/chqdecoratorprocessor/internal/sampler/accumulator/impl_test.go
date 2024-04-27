package accumulator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAggregatorImpl(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	aggregator := NewAggregatorImpl(buckets)

	assert.NotNil(t, aggregator)
	assert.Equal(t, buckets, aggregator.buckets)
	assert.Equal(t, uint64(0), aggregator.count)
	assert.Equal(t, make([]int64, len(buckets)), aggregator.sum)
}

func TestAggregatorImpl_Add(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	aggregator := NewAggregatorImpl(buckets)

	// Test case 1: Add valid value
	value := []int64{10, 20, 30, 40, 50}
	err := aggregator.Add(value)
	assert.NoError(t, err)
	err = aggregator.Add([]int64{1, 2, 3, 4, 5})
	assert.NoError(t, err)
	assert.Equal(t, []int64{11, 22, 33, 44, 55}, aggregator.sum)
	assert.Equal(t, uint64(2), aggregator.count)

	// Test case 2: Add value with incorrect length
	value = []int64{10, 20, 30, 40}
	err = aggregator.Add(value)
	assert.Error(t, err)
	assert.Equal(t, []int64{11, 22, 33, 44, 55}, aggregator.sum)
	assert.Equal(t, uint64(2), aggregator.count)
}

func TestAggregatorImpl_Buckets(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	aggregator := NewAggregatorImpl(buckets)

	assert.Equal(t, buckets, aggregator.Buckets())
}

func TestAggregatorImpl_Count(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	aggregator := NewAggregatorImpl(buckets)

	assert.Equal(t, uint64(0), aggregator.Count())

	aggregator.count = 10
	assert.Equal(t, uint64(10), aggregator.Count())
}

func TestAggregatorImpl_Sum(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	aggregator := NewAggregatorImpl(buckets)

	assert.Equal(t, make([]int64, len(buckets)), aggregator.Sum())

	aggregator.sum = []int64{10, 20, 30, 40, 50}
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, aggregator.Sum())
}

func TestAggregatorImpl_Avg(t *testing.T) {
	buckets := []int64{1, 2, 3, 4, 5}
	aggregator := NewAggregatorImpl(buckets)

	// Test case 1: Empty aggregator
	assert.Equal(t, make([]int64, len(buckets)), aggregator.Avg())

	// Test case 2: Non-empty aggregator
	aggregator.sum = []int64{10, 20, 30, 40, 50}
	aggregator.count = 5
	assert.Equal(t, []int64{2, 4, 6, 8, 10}, aggregator.Avg())
}
