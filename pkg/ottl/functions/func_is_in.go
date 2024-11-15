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

type IsInArguments[K any] struct {
	Target ottl.StringGetter[K]
	List   []string
}

func NewIsInFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("IsIn", &IsInArguments[K]{}, createIsInFunction[K])
}

func createIsInFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*IsInArguments[K])
	if !ok {
		return nil, fmt.Errorf("IsInFactory args must be of type *IsInArguments[K]")
	}

	// Convert the list to a map for O(1) lookup
	lookupMap := make(map[string]struct{}, len(args.List))
	for _, item := range args.List {
		lookupMap[item] = struct{}{}
	}

	return isIn(args.Target, lookupMap), nil
}

func isIn[K any](target ottl.StringGetter[K], lookupMap map[string]struct{}) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		str, err := target.Get(ctx, tCtx)
		if err != nil {
			return false, err
		}
		_, exists := lookupMap[str]
		return exists, nil
	}
}
