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

package timebox

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateInterval(t *testing.T) {

	tests := []struct {
		now      int64
		interval int64
		expected int64
	}{
		{now: 1000, interval: 100, expected: 1000},
		{now: 1001, interval: 100, expected: 1000},
		{now: 1000, interval: 1000, expected: 1000},
		{now: 1001, interval: 1000, expected: 1000},
		{now: 2001, interval: 1000, expected: 2000},
		{now: 1718827732167, interval: 10000, expected: 1718827730000},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("index %d", i), func(t *testing.T) {
			assert.Equal(t, tt.expected, CalculateInterval(tt.now, tt.interval))
		})
	}
}
