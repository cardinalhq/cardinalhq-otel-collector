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

import "fmt"

type Aggregation[T int64 | float64] struct {
	AggregationType string
	Name            string
	Tags            map[string]string
	Sum             T
	Count           uint64
}

func NewAggregation[T int64 | float64](name string, aggregationType string, tags map[string]string) *Aggregation[T] {
	return &Aggregation[T]{
		Name:            name,
		AggregationType: aggregationType,
		Tags:            tags,
	}
}

func (a *Aggregation[T]) Add(name string, value T) error {
	if a.Name != name {
		return fmt.Errorf("aggregation name mismatch: %s != %s", a.Name, name)
	}
	a.Sum += value
	a.Count++
	return nil
}

func (a *Aggregation[T]) Value() T {
	if a.AggregationType == "avg" {
		return a.Sum / T(a.Count)
	}
	return a.Sum
}
