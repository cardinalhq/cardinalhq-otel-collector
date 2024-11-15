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
	"errors"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/stretchr/testify/assert"
)

func Test_IsIn(t *testing.T) {
	tests := []struct {
		name      string
		target    any
		list      []string
		expected  bool
		shouldErr bool
	}{
		{
			name:     "Target is in the list",
			target:   "apple",
			list:     []string{"apple", "banana", "cherry"},
			expected: true,
		},
		{
			name:     "Target is not in the list",
			target:   "orange",
			list:     []string{"apple", "banana", "cherry"},
			expected: false,
		},
		{
			name:     "Empty list",
			target:   "apple",
			list:     []string{},
			expected: false,
		},
		{
			name:     "Empty target and list contains empty string",
			target:   "",
			list:     []string{"", "banana", "cherry"},
			expected: true,
		},
		{
			name:      "Nil target",
			target:    nil,
			list:      []string{"apple", "banana", "cherry"},
			shouldErr: true,
		},
		{
			name:      "Error getting target",
			target:    "apple",
			list:      []string{"apple", "banana", "cherry"},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var targetGetter ottl.StringGetter[any]

			if tt.shouldErr {
				targetGetter = &ottl.StandardStringGetter[any]{
					Getter: func(context.Context, any) (any, error) {
						return nil, errors.New("target error")
					},
				}
			} else {
				targetGetter = &ottl.StandardStringGetter[any]{
					Getter: func(context.Context, any) (any, error) {
						return tt.target, nil
					},
				}
			}

			args := &IsInArguments[any]{
				Target: targetGetter,
				List:   tt.list,
			}

			funcCtx := ottl.FunctionContext{}

			exprFunc, err := NewIsInFactory[any]().CreateFunction(funcCtx, args)
			assert.NoError(t, err)

			result, err := exprFunc(context.Background(), nil)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Equal(t, false, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func Test_IsIn_Error(t *testing.T) {
	targetGetter := &ottl.StandardStringGetter[any]{
		Getter: func(context.Context, any) (any, error) {
			return nil, ottl.TypeError("target error")
		},
	}
	args := &IsInArguments[any]{
		Target: targetGetter,
		List:   []string{"apple", "banana"},
	}

	funcCtx := ottl.FunctionContext{}
	exprFunc, err := NewIsInFactory[any]().CreateFunction(funcCtx, args)
	assert.NoError(t, err)

	result, err := exprFunc(context.Background(), nil)
	assert.Equal(t, false, result)
	assert.Error(t, err)
	var typeError ottl.TypeError
	ok := errors.As(err, &typeError)
	assert.True(t, ok)
}
