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

package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockStatsObject struct {
	wasInitialized  bool
	incrementCalled bool
	count           int64
	name            string
}

func (m *mockStatsObject) Key() uint64 { return 0 }

func (m *mockStatsObject) Matches(other StatsObject) bool {
	if o, ok := other.(*mockStatsObject); ok {
		return m.name == o.name
	}
	return false
}

func (m *mockStatsObject) Increment(_ string, count int, _ int64) error {
	m.incrementCalled = true
	m.count += int64(count)
	return nil
}

func (m *mockStatsObject) Initialize() error {
	m.wasInitialized = true
	return nil
}

func TestLogStats_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		item          StatsObject
		wantCount     int64
		wantInit      bool
		wantIncrement bool
	}{
		{
			"increment count",
			&mockStatsObject{},
			0,
			true,
			true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.item.Increment("", i, int64(i))
			assert.NoError(t, err)
			item, ok := tt.item.(*mockStatsObject)
			assert.True(t, ok)
			assert.Equal(t, tt.wantCount, item.count, "count mismatch")
			assert.Equal(t, tt.wantInit, item.wasInitialized, "initialize called")
			assert.Equal(t, tt.wantIncrement, item.incrementCalled, "increment called")
		})
	}
}
