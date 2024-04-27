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

package sampler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregation_Add(t *testing.T) {
	aggregation := &Aggregation[float64]{}

	aggregation.Add(10.5)
	assert.Equal(t, 10.5, aggregation.Sum)
	assert.Equal(t, uint64(1), aggregation.Count)

	aggregation.Add(5.5)
	assert.Equal(t, 16.0, aggregation.Sum)
	assert.Equal(t, uint64(2), aggregation.Count)
}

func TestAggregation_Value_avg(t *testing.T) {
	aggregation := &Aggregation[float64]{
		AggregationType: "avg",
	}

	aggregation.Add(10.5)
	aggregation.Add(5.5)
	assert.Equal(t, 8.0, aggregation.Value())
}

func TestAggregation_Value_sum(t *testing.T) {
	aggregation := &Aggregation[float64]{
		AggregationType: "sum",
	}

	aggregation.Add(10.5)
	aggregation.Add(5.5)
	assert.Equal(t, 16.0, aggregation.Value())
}
