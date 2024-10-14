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
)

func TestAggregationImpl_Add(t *testing.T) {
	aggregation := NewAggregationImpl("alice", []float64{1}, AggregationTypeSum, nil)

	err := aggregation.Add("alice", []float64{10.5})
	assert.Nil(t, err)
	assert.Equal(t, []float64{10.5}, aggregation.Value())
	assert.Equal(t, uint64(1), aggregation.Count())

	err = aggregation.Add("alice", []float64{5.5})
	assert.Nil(t, err)
	assert.Equal(t, []float64{16.0}, aggregation.Value())
	assert.Equal(t, uint64(2), aggregation.Count())
	assert.Equal(t, "alice", aggregation.Name())
}

func TestAggregationImpl_Value_avg(t *testing.T) {
	aggregation := NewAggregationImpl("alice", []float64{1}, AggregationTypeAvg, nil)

	err := aggregation.Add("alice", []float64{10.5})
	assert.Nil(t, err)
	err = aggregation.Add("alice", []float64{5.5})
	assert.Nil(t, err)
	assert.Equal(t, []float64{8.0}, aggregation.Value())
	assert.Equal(t, uint64(2), aggregation.Count())
	assert.Equal(t, "alice", aggregation.Name())
}

func TestAggregationImpl_Value_sum(t *testing.T) {
	aggregation := NewAggregationImpl("alice", []float64{1}, AggregationTypeSum, nil)

	err := aggregation.Add("alice", []float64{10.5})
	assert.Nil(t, err)
	err = aggregation.Add("alice", []float64{5.5})
	assert.Nil(t, err)
	assert.Equal(t, []float64{16.0}, aggregation.Value())
	assert.Equal(t, "alice", aggregation.Name())
}
