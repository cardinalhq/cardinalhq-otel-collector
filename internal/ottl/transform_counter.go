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
	"context"

	"go.opentelemetry.io/otel/metric"
)

type TransformCounter struct {
	counter    metric.Int64Counter
	addOptions []metric.AddOption
}

var _ DeferrableCounter = (*TransformCounter)(nil)

func (tc *TransformCounter) Add(delta int64, options ...metric.AddOption) {
	tc.counter.Add(context.Background(), delta, append(tc.addOptions, options...)...)
}

func NewTransformCounter(meter metric.Meter, name string, counterOptions []metric.Int64CounterOption, addOptions []metric.AddOption) (*TransformCounter, error) {
	counter, err := meter.Int64Counter(name, counterOptions...)
	if err != nil {
		return nil, err
	}
	return &TransformCounter{
		counter:    counter,
		addOptions: addOptions,
	}, nil
}
