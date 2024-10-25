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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock Getter to simulate the behavior of a Getter in the OTTL function
type mockGetter[K any] struct {
	value interface{}
	err   error
}

func (m *mockGetter[K]) Get(_ context.Context, _ K) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.value, nil
}

func Test_Exists(t *testing.T) {
	tests := []struct {
		name      string
		target    interface{}
		shouldErr bool
		expected  bool
	}{
		{
			name:     "Target value exists (non-nil)",
			target:   "existingValue",
			expected: true,
		},
		{
			name:     "Target value does not exist (nil)",
			target:   nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock the Getter to return the target value or simulate an error
			var getter ottl.Getter[any]
			if tt.shouldErr {
				getter = &mockGetter[any]{
					value: nil,
					err:   errors.New("mock error"),
				}
			} else {
				getter = &mockGetter[any]{
					value: tt.target,
					err:   nil,
				}
			}

			// Create the arguments and function context for the has function
			args := &existsArguments[any]{
				Target: getter,
			}
			funcCtx := ottl.FunctionContext{}

			// Create the has() function
			hasFunc, err := NewExistsFactory[any]().CreateFunction(funcCtx, args)
			assert.NoError(t, err)

			// Execute the has function and check the result
			result, err := hasFunc(context.Background(), nil)
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
