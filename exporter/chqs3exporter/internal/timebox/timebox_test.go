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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimebox(t *testing.T) {
	tests := []struct {
		name     string
		t        int64
		interval int64
		want     int64
	}{
		{"99 % 10", 99, 10, 90},
		{"100 % 10", 100, 10, 100},
		{"101 % 10", 101, 10, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Timebox(tt.t, tt.interval)
			assert.Equal(t, tt.want, got)
		})
	}
}
