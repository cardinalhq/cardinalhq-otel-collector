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

package functions

import (
	"context"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type existsArguments[K any] struct {
	Target ottl.Getter[K] `ottlarg:"0"`
}

func NewExistsFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("Exists", &existsArguments[K]{}, createExistsFunction[K])
}

func createExistsFunction[K any](_ ottl.FunctionContext, a ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := a.(*existsArguments[K])
	if !ok {
		return nil, fmt.Errorf("hasFactory args must be of type *existsArguments[K]")
	}

	return existsFn[K](args)
}

func existsFn[K any](c *existsArguments[K]) (ottl.ExprFunc[K], error) {
	return func(ctx context.Context, tCtx K) (interface{}, error) {
		got, err := c.Target.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}
		return got != nil, nil
	}, nil
}
