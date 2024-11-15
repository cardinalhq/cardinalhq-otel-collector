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

func Test_CidrMatch(t *testing.T) {
	tests := []struct {
		name     string
		subnet   string
		ip       string
		expected bool
	}{
		{
			name:     "IP within CIDR subnet",
			subnet:   "34.20.223.128/25",
			ip:       "34.20.223.130",
			expected: true,
		},
		{
			name:     "IP outside CIDR subnet",
			subnet:   "34.20.223.128/25",
			ip:       "34.20.224.1",
			expected: false,
		},
		{
			name:     "Invalid CIDR subnet",
			subnet:   "invalid-cidr",
			ip:       "34.20.223.130",
			expected: false,
		},
		{
			name:     "Invalid IP address",
			subnet:   "34.20.223.128/25",
			ip:       "invalid-ip",
			expected: false,
		},
		{
			name:     "IP within a different CIDR subnet",
			subnet:   "10.0.0.0/8",
			ip:       "10.0.1.1",
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprFunc := cidrMatch[any](&ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return tt.subnet, nil
				},
			}, &ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return tt.ip, nil
				},
			})
			result, err := exprFunc(context.Background(), nil)
			if tt.subnet == "invalid-cidr" || tt.ip == "invalid-ip" {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_CidrMatch_Error(t *testing.T) {
	exprFunc := cidrMatch[any](&ottl.StandardStringGetter[any]{
		Getter: func(context.Context, any) (any, error) {
			return "", ottl.TypeError("subnet error")
		},
	}, &ottl.StandardStringGetter[any]{
		Getter: func(context.Context, any) (any, error) {
			return "", nil
		},
	})
	result, err := exprFunc(context.Background(), nil)
	assert.Equal(t, false, result)
	assert.Error(t, err)
	var typeError ottl.TypeError
	ok := errors.As(err, &typeError)
	assert.False(t, ok)
}
