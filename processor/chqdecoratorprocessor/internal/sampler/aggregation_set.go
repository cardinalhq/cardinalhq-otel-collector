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

type AggregationSet[T int64 | float64] struct {
	Aggregations map[uint64]*Aggregation[T]
	StartTime    int64
	Interval     int64
}

func NewAggregationSet[T int64 | float64](starttime int64, interval int64) *AggregationSet[T] {
	return &AggregationSet[T]{
		Aggregations: map[uint64]*Aggregation[T]{},
		StartTime:    starttime,
		Interval:     interval,
	}
}

func (a *AggregationSet[T]) Add(name string, value T, aggregationType string, tags map[string]string) error {
	fingerprint := FingerprintTags(tags)
	if _, ok := a.Aggregations[fingerprint]; !ok {
		a.Aggregations[fingerprint] = NewAggregation[T](name, aggregationType, tags)
	}
	return a.Aggregations[fingerprint].Add(name, value)
}

func (a *AggregationSet[T]) GetAggregations() map[uint64]*Aggregation[T] {
	return a.Aggregations
}
