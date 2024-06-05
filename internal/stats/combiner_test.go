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
	"time"

	"github.com/stretchr/testify/assert"
)

type mockStatsObject struct {
	wasInitialized  bool
	incrementCalled bool
	count           int64
	name            string
	key             uint64
}

func (m *mockStatsObject) Key() uint64 { return m.key }

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

func TestLogStats_Record_Single_Match(t *testing.T) {
	t.Parallel()

	now := time.Now()

	combiner := NewStatsCombiner[*mockStatsObject](now, time.Second*10)
	item := &mockStatsObject{name: "test", key: 1, count: 1}
	item2 := &mockStatsObject{name: "test", key: 1, count: 1}
	buckets, err := combiner.Record(now, item, "", 1, 1)
	assert.NoError(t, err)
	assert.Nil(t, buckets)

	buckets, err = combiner.Record(now.Add(1000*time.Second), item2, "", 1, 1)
	assert.NoError(t, err)
	assert.NotNil(t, buckets)

	// item 1 will be initialized, and incremented by adding item2
	assert.True(t, item.incrementCalled)
	assert.True(t, item.wasInitialized)

	// item 2 will not be initialized, as it will be equal to item 1, and not otherwise touched
	assert.False(t, item2.incrementCalled)
	assert.False(t, item2.wasInitialized)

	// only one bucket will be returned as we forced this by using the same key.
	assert.Equal(t, 1, len(*buckets))

	for _, bucket := range *buckets {
		assert.Equal(t, 1, len(bucket))
		b := bucket[0]
		assert.Equal(t, int64(2), b.count)
	}
}

func TestLogStats_Record_Multiple_Keys(t *testing.T) {
	t.Parallel()

	now := time.Now()

	combiner := NewStatsCombiner[*mockStatsObject](now, time.Second*10)
	item := &mockStatsObject{name: "test", key: 1, count: 1}
	item2 := &mockStatsObject{name: "test2", key: 2, count: 1}
	buckets, err := combiner.Record(now, item, "", 1, 1)
	assert.NoError(t, err)
	assert.Nil(t, buckets)

	buckets, err = combiner.Record(now.Add(1000*time.Second), item2, "", 1, 1)
	assert.NoError(t, err)
	assert.NotNil(t, buckets)

	// item 1 will be initialized
	assert.False(t, item.incrementCalled)
	assert.True(t, item.wasInitialized)

	// item 2 will be initialized
	assert.False(t, item2.incrementCalled)
	assert.True(t, item2.wasInitialized)

	assert.Equal(t, 2, len(*buckets))

	for _, bucket := range *buckets {
		assert.Equal(t, 1, len(bucket))
		b := bucket[0]
		assert.Equal(t, int64(1), b.count)
	}
}

func TestLogStats_Record_Single_Key(t *testing.T) {
	t.Parallel()

	now := time.Now()

	combiner := NewStatsCombiner[*mockStatsObject](now, time.Second*10)
	item := &mockStatsObject{name: "test", key: 1, count: 1}
	item2 := &mockStatsObject{name: "test2", key: 1, count: 1}
	buckets, err := combiner.Record(now, item, "", 1, 1)
	assert.NoError(t, err)
	assert.Nil(t, buckets)

	buckets, err = combiner.Record(now.Add(1000*time.Second), item2, "", 1, 1)
	assert.NoError(t, err)
	assert.NotNil(t, buckets)

	// item 1 will be initialized
	assert.False(t, item.incrementCalled)
	assert.True(t, item.wasInitialized)

	// item 2 will be initialized
	assert.False(t, item2.incrementCalled)
	assert.True(t, item2.wasInitialized)

	assert.Equal(t, 1, len(*buckets))

	for _, bucket := range *buckets {
		assert.Equal(t, 2, len(bucket))
		assert.Equal(t, int64(1), bucket[0].count)
		assert.Equal(t, int64(1), bucket[1].count)
	}
}
