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
	"time"

	"github.com/stretchr/testify/assert"
)

type MockEntry struct {
	Value int
}

func (m *MockEntry) Encode() ([]byte, error) {
	return []byte{byte(m.Value)}, nil
}

func (m *MockEntry) New(b []byte) (Entry, error) {
	return &MockEntry{Value: int(b[0])}, nil
}

func TestNewScopedTimeboxImpl(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)

	assert.Equal(t, interval, timebox.Interval)
	assert.Equal(t, intervalCount, timebox.IntervalCount)
	assert.Equal(t, grace, timebox.Grace)
	assert.NotNil(t, timebox.items)
	assert.Empty(t, timebox.items)
}

func TestScopedTimeboxImpl_Append(t *testing.T) {
	intervalTS := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	tbox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), intervalTS, intervalCount, grace)

	scope := 1
	ts := int64(1234567890)
	tboxKey := CalculateInterval(ts, intervalTS)
	item := &MockEntry{1}

	err := tbox.Append(scope, ts, item)
	assert.NoError(t, err)

	itemCount := 0
	foundItems := map[int64][]*MockEntry{}
	err = tbox.ForEach(scope, &MockEntry{}, func(ts int64, items map[int64][]*MockEntry) bool {
		itemCount++
		foundItems = items
		return true
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, itemCount)

	entry := foundItems[tboxKey]
	assert.Equal(t, 1, len(entry))
	assert.Equal(t, item, entry[0])
}

func TestScopedTimeboxImpl_ItemCount(t *testing.T) {
	interval := int64(10000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope := 1
	ts := time.Now().UnixMilli()
	tbox := CalculateInterval(ts, interval)
	item := &MockEntry{}
	err := timebox.Append(scope, ts, item)
	assert.NoError(t, err)

	count := timebox.ItemCount(scope, tbox)
	assert.Equal(t, 1, count)
}

func TestScopedTimeboxImpl_ItemCountMissingScope(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope := 1
	ts := int64(1234567890)
	count := timebox.ItemCount(scope, ts)
	assert.Equal(t, 0, count)
}

func TestScopedTimeboxImpl_ItemCountMissingTS(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope := 1
	ts := int64(1234567890)
	item := &MockEntry{}
	err := timebox.Append(scope, ts, item)
	assert.NoError(t, err)

	count := timebox.ItemCount(scope, ts-1)
	assert.Equal(t, 0, count)
}

func TestScopedTimeboxImpl_Closed(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(1)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope := 1
	now := int64(1234567890)
	ts := now - interval - grace
	tbox := CalculateInterval(ts, interval)
	item := &MockEntry{}
	err := timebox.Append(scope, ts, item)
	assert.NoError(t, err)

	closedItems, err := timebox.Closed(scope, now, &MockEntry{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(closedItems))
	assert.Equal(t, item, closedItems[tbox][0])

	// Ensure the item is removed from the timebox
	found := false
	err = timebox.ForEach(scope, &MockEntry{}, func(ts int64, items map[int64][]*MockEntry) bool {
		if len(items) > 0 {
			found = true
		}
		return true
	})
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestScopedTimeboxImpl_Closed_IntervalCount2(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope := 1
	now := int64(1234567890)
	ts := now - interval - grace
	item := &MockEntry{}
	err := timebox.Append(scope, ts, item)
	assert.NoError(t, err)

	closedItems, err := timebox.Closed(scope, now, &MockEntry{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(closedItems))
}

func TestScopedTimeboxImpl_ClosedMissingScope(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope := 1
	now := int64(1234567890)
	closedItems, err := timebox.Closed(scope, now, &MockEntry{})
	assert.NoError(t, err)
	assert.Empty(t, closedItems)
}

func TestScopedTimeboxImpl_Scopes(t *testing.T) {
	interval := int64(1000)
	grace := int64(200)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *MockEntry](NewMemoryBufferFactory(), interval, intervalCount, grace)
	scope1 := 1
	scope2 := 2
	scope3 := 3
	err := timebox.Append(scope1, 1234567890, &MockEntry{})
	assert.NoError(t, err)
	err = timebox.Append(scope2, 1234567890, &MockEntry{})
	assert.NoError(t, err)
	err = timebox.Append(scope3, 1234567890, &MockEntry{})
	assert.NoError(t, err)
	scopes := timebox.Scopes()
	assert.ElementsMatch(t, []int{scope1, scope2, scope3}, scopes)
}

func TestTimeboxImpl_TooOld(t *testing.T) {
	interval := int64(1000)
	grace := int64(0)
	intervalCount := int64(2)
	timebox := NewTimeboxImpl[int, *mm](nil, interval, intervalCount, grace)

	// Test when ts is too old
	for i := int64(0); i < 100; i++ {
		ts := int64(500)
		now := ts + interval*intervalCount + grace + 10 + (i * interval)
		tooOld, remaining := timebox.TooOld(ts, now)
		assert.True(t, tooOld)
		assert.Equal(t, i+1, remaining)
	}

	// Test when ts is not too old
	ts := int64(1000)
	now := ts + interval*intervalCount + grace - 10
	tooOld, remaining := timebox.TooOld(ts, now)
	assert.False(t, tooOld)
	assert.Equal(t, int64(0), remaining)
}
