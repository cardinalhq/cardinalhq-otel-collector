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
	"go.uber.org/zap"
)

func TestAggregationSet_Add(t *testing.T) {
	aggregationSet := &AggregationSet[float64]{
		Aggregations: make(map[uint64]*AggregationImpl[float64]),
	}

	values := []float64{10.5}
	buckets := []float64{1}
	aggregationType := AggregationTypeAvg
	tags := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	fingerprint := FingerprintTags(tags)

	err := aggregationSet.Add(zap.NewNop(), "alice", buckets, values, aggregationType, tags)
	assert.Nil(t, err)

	assert.NotNil(t, aggregationSet.Aggregations[fingerprint])
	assert.Equal(t, values, aggregationSet.Aggregations[fingerprint].Value())
	assert.Equal(t, uint64(1), aggregationSet.Aggregations[fingerprint].Count())
	assert.Equal(t, aggregationType, aggregationSet.Aggregations[fingerprint].AggregationType())
	assert.Equal(t, tags, aggregationSet.Aggregations[fingerprint].Tags())
	assert.Equal(t, "alice", aggregationSet.Aggregations[fingerprint].Name())
}

func TestAggregationSet_Add_ExistingAggregation(t *testing.T) {
	buckets := []float64{1}
	aggregationSet := NewAggregationSet[float64](1234567890, 60)

	value1 := 10.5
	value2 := 5.5
	aggregationType := AggregationTypeSum
	tags := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	fingerprint := FingerprintTags(tags)

	ai := NewAggregationImpl(
		"alice",
		buckets,
		aggregationType,
		tags,
	)
	err := ai.Add("alice", []float64{value1})
	assert.Nil(t, err)

	aggregationSet.Aggregations[fingerprint] = ai
	err = aggregationSet.Add(zap.NewNop(), "alice", buckets, []float64{value2}, aggregationType, tags)
	assert.Nil(t, err)

	assert.NotNil(t, aggregationSet.Aggregations[fingerprint])
	assert.Equal(t, []float64{value1 + value2}, aggregationSet.Aggregations[fingerprint].Value())
	assert.Equal(t, buckets, aggregationSet.Aggregations[fingerprint].Buckets())
	assert.Equal(t, uint64(2), aggregationSet.Aggregations[fingerprint].Count())
	assert.Equal(t, aggregationType, aggregationSet.Aggregations[fingerprint].AggregationType())
	assert.Equal(t, tags, aggregationSet.Aggregations[fingerprint].Tags())
	assert.Equal(t, "alice", aggregationSet.Aggregations[fingerprint].Name())
}

func TestNewAggregationSet(t *testing.T) {
	starttime := int64(1234567890)
	interval := int64(60)

	aggregationSet := NewAggregationSet[float64](starttime, interval)

	assert.NotNil(t, aggregationSet)
	assert.Equal(t, map[uint64]*AggregationImpl[float64]{}, aggregationSet.Aggregations)
	assert.Equal(t, starttime, aggregationSet.StartTime)
	assert.Equal(t, interval, aggregationSet.Interval)
}

func TestAggregationSet_GetAggregations(t *testing.T) {
	aggregationSet := &AggregationSet[float64]{
		Aggregations: make(map[uint64]*AggregationImpl[float64]),
	}

	// Add some sample aggregations
	aggregationSet.Aggregations[1] = &AggregationImpl[float64]{}
	aggregationSet.Aggregations[2] = &AggregationImpl[float64]{}
	aggregationSet.Aggregations[3] = &AggregationImpl[float64]{}

	aggregations := aggregationSet.GetAggregations()

	assert.NotNil(t, aggregations)
	assert.Equal(t, 3, len(aggregations))
	assert.Equal(t, aggregationSet.Aggregations[1], aggregations[1])
	assert.Equal(t, aggregationSet.Aggregations[2], aggregations[2])
	assert.Equal(t, aggregationSet.Aggregations[3], aggregations[3])
}
