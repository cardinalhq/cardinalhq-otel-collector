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

package boxer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBox_tooOld(t *testing.T) {
	interval := time.Second
	intervalCount := int64(2)
	grace := time.Millisecond * 200
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	now := timefunc()

	tests := []struct {
		name     string
		ts       time.Time
		expected bool
	}{
		{
			"early",
			now.Add(-interval - grace),
			false,
		},
		{
			"one interval from now",
			now.Add(-interval),
			false,
		},
		{
			"two intervals from now",
			now.Add(-interval * 2),
			false,
		},
		{
			"three intervals from now",
			now.Add(-interval * 3),
			true,
		},
		{
			"just under three intervals from now with grace",
			now.Add(-interval*2 - grace),
			false,
		},
		{
			"just into the third interval from now after grace",
			now.Add(-interval*2 - grace - 1),
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvs := NewMemoryKVS(nil)
			box, err := NewBoxer(WithKVS(kvs), WithInterval(interval), WithIntervalCount(intervalCount), WithGrace(grace), WithTimeFunc(timefunc))
			assert.NoError(t, err)
			result := box.tooOld(tt.ts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_intervalTooOld(t *testing.T) {
	interval := time.Second
	intervalCount := int64(2)
	grace := time.Millisecond * 200
	timefunc := func() time.Time { return time.Unix(1000, 1000) }

	tests := []struct {
		name     string
		interval int64
		expected bool
	}{
		{"very early", 0, true},
		{"just shy of grace", 997, false},
		{"-2 from now", 998, false},
		{"-1 from now", 999, false},
		{"now", 1000, false},
		{"one interval from now", 1001, false},
		{"two intervals from now", 1002, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvs := NewMemoryKVS(nil)
			box, err := NewBoxer(WithKVS(kvs), WithInterval(interval), WithIntervalCount(intervalCount), WithGrace(grace), WithTimeFunc(timefunc))
			assert.NoError(t, err)
			result := box.intervalTooOld(tt.interval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_intervalNumber(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTTL(time.Hour))
	assert.NoError(t, err)
	tests := []struct {
		name     string
		ts       time.Time
		expected int64
	}{
		{
			"epoch",
			time.Unix(0, 0),
			0,
		},
		{
			"one interval",
			time.Unix(1, 0),
			1,
		},
		{
			"two intervals",
			time.Unix(2, 0),
			2,
		},
		{
			"one interval and some nanoseconds",
			time.Unix(1, 500_000_000),
			1,
		},
		{
			"two intervals and some nanoseconds",
			time.Unix(2, 500_000_000),
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := box.IntervalForTime(tt.ts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_generatePrefix(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTTL(time.Hour))
	assert.NoError(t, err)
	tests := []struct {
		name     string
		scope    string
		ts       time.Time
		expected []byte
	}{
		{
			"test1",
			"scope1",
			time.Unix(0, 0),
			[]byte("0-scope1-"),
		},
		{
			"test2",
			"scope2",
			time.Unix(1, 0),
			[]byte("1-scope2-"),
		},
		{
			"test3",
			"scope3",
			time.Unix(2, 0),
			[]byte("2-scope3-"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := box.generatePrefix(tt.scope, tt.ts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_generateFullKey(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTTL(time.Hour))
	assert.NoError(t, err)
	tests := []struct {
		name           string
		scope          string
		ts             time.Time
		expectedPrefix []byte
	}{
		{
			"test1",
			"scope1",
			time.Unix(0, 0),
			[]byte("0-scope1-"),
		},
		{
			"test2",
			"scope2",
			time.Unix(1, 0),
			[]byte("1-scope2-"),
		},
		{
			"test3",
			"scope3",
			time.Unix(2, 0),
			[]byte("2-scope3-"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := box.generateFullKey(tt.scope, tt.ts)
			assert.Greater(t, len(result), len(tt.expectedPrefix))
			assert.Equal(t, tt.expectedPrefix, result[:len(tt.expectedPrefix)])
		})
	}
}

func TestBox_generateFullKey_withSuffix(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTTL(time.Hour), WithKeySuffix("martin"))
	assert.NoError(t, err)
	tests := []struct {
		name           string
		scope          string
		ts             time.Time
		expectedPrefix []byte
		expectedSuffix []byte
	}{
		{
			"test1",
			"scope1",
			time.Unix(0, 0),
			[]byte("0-scope1-"),
			[]byte("-martin"),
		},
		{
			"test2",
			"scope2",
			time.Unix(1, 0),
			[]byte("1-scope2-"),
			[]byte("-martin"),
		},
		{
			"test3",
			"scope3",
			time.Unix(2, 0),
			[]byte("2-scope3-"),
			[]byte("-martin"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := box.generateFullKey(tt.scope, tt.ts)
			assert.Greater(t, len(result), len(tt.expectedPrefix))
			assert.Equal(t, tt.expectedPrefix, result[:len(tt.expectedPrefix)])
			assert.Equal(t, tt.expectedSuffix, result[len(result)-len(tt.expectedSuffix):])
		})
	}
}

func TestBox_Put(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTTL(time.Hour), WithTimeFunc(timefunc))
	assert.NoError(t, err)
	tests := []struct {
		name           string
		scope          string
		ts             time.Time
		value          []byte
		expectedPrefix []byte
		expectedAdded  bool
	}{
		{
			"test1",
			"scope1",
			time.Unix(0, 0),
			[]byte("value1"),
			[]byte{},
			false,
		},
		{
			"test2",
			"scope2",
			time.Unix(1001, 0),
			[]byte("value2"),
			[]byte("1001-scope2-"),
			true,
		},
		{
			"test3",
			"scope3",
			time.Unix(1000, 0),
			[]byte("value3"),
			[]byte("1000-scope3-"),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := box.Put(tt.scope, tt.ts, tt.value)
			assert.NoError(t, err)
			if tt.expectedAdded {
				assert.Greater(t, len(key), len(tt.expectedPrefix))
				assert.Equal(t, tt.expectedPrefix, key[:len(tt.expectedPrefix)])
			} else {
				assert.Equal(t, 0, len(key))
			}
		})
	}
}

func TestSanitizeScope(t *testing.T) {
	tests := []struct {
		name     string
		scope    string
		expected string
	}{
		{
			"no special characters",
			"abc123",
			"abc123",
		},
		{
			"with special characters",
			"abc!@#123",
			"abc___123",
		},
		{
			"with spaces",
			"abc 123",
			"abc_123",
		},
		{
			"with uppercase letters",
			"ABC123",
			"ABC123",
		},
		{
			"with mixed case letters",
			"aBc-123",
			"aBc_123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeScope(tt.scope)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_ForEachScope(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithTimeFunc(timefunc))
	assert.NoError(t, err)
	tests := []struct {
		name         string
		items        map[string]string
		scope        string
		expected     []string
		expectedKeys []string
	}{
		{
			"no items",
			map[string]string{},
			"scope1",
			[]string{},
			[]string{},
		},
		{
			"one item different scope",
			map[string]string{
				"1000-scope2-100000000000": "value2",
			},
			"scope1",
			[]string{},
			[]string{},
		},
		{
			"one item same scope",
			map[string]string{
				"1000-scope1-100000000000": "value1",
			},
			"scope1",
			[]string{"value1"},
			[]string{"1000-scope1-100000000000"},
		},
		{
			"two items same scope",
			map[string]string{
				"1000-scope1-100000000000": "value1",
				"1000-scope1-100000000001": "value2",
			},
			"scope1",
			[]string{"value1", "value2"},
			[]string{"1000-scope1-100000000000", "1000-scope1-100000000001"},
		},
		{
			"same scope different timeboxes",
			map[string]string{
				"1000-scope1-100000000000": "value1",
				"1001-scope1-100000000000": "value2",
			},
			"scope1",
			[]string{"value1"},
			[]string{"1000-scope1-100000000000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if w, ok := kvs.(Wiper); ok {
				err := w.Wipe()
				assert.NoError(t, err)
			} else {
				t.Fatal("kvs does not implement Wiper")
			}
			for key, value := range tt.items {
				err := kvs.Set([]byte(key), []byte(value), 0)
				assert.NoError(t, err)
			}

			result := []string{}
			resultKeys := []string{}
			err := box.ForEachScope(tt.scope, timefunc(), func(scope string, ts time.Time, key []byte, value []byte) bool {
				result = append(result, string(value))
				resultKeys = append(resultKeys, string(key))
				return true
			})
			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, result)
			assert.ElementsMatch(t, tt.expectedKeys, resultKeys)
		})
	}
}

func TestBox_ForEach(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithTimeFunc(timefunc))
	assert.NoError(t, err)
	tests := []struct {
		name         string
		items        map[string]string
		tbox         int64
		expected     []string
		expectedKeys []string
	}{
		{
			"no items",
			map[string]string{},
			1000,
			[]string{},
			[]string{},
		},
		{
			"one item different tbox",
			map[string]string{
				"1000-scope2-100000000000": "value2",
			},
			1001,
			[]string{},
			[]string{},
		},
		{
			"one item, this tbox",
			map[string]string{
				"1000-scope1-100000000000": "value1",
			},
			1000,
			[]string{"value1"},
			[]string{"1000-scope1-100000000000"},
		},
		{
			"two items same scope",
			map[string]string{
				"1000-scope1-100000000000": "value1",
				"1000-scope1-100000000001": "value2",
			},
			1000,
			[]string{"value1", "value2"},
			[]string{"1000-scope1-100000000000", "1000-scope1-100000000001"},
		},
		{
			"multiple scopes and tboxes",
			map[string]string{
				"1000-scope1-100000000000": "value1",
				"1001-scope1-100000000000": "value2",
				"1000-scope2-100000000000": "value3",
				"1001-scope2-100000000000": "value4",
			},
			1000,
			[]string{"value1", "value3"},
			[]string{"1000-scope1-100000000000", "1000-scope2-100000000000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if w, ok := kvs.(Wiper); ok {
				err := w.Wipe()
				assert.NoError(t, err)
			} else {
				t.Fatal("kvs does not implement Wiper")
			}
			for key, value := range tt.items {
				err := kvs.Set([]byte(key), []byte(value), 0)
				assert.NoError(t, err)
			}

			result := []string{}
			resultKeys := []string{}
			err := box.ForEach(tt.tbox, func(scope string, ts time.Time, key []byte, value []byte) bool {
				result = append(result, string(value))
				resultKeys = append(resultKeys, string(key))
				return true
			})
			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, result)
			assert.ElementsMatch(t, tt.expectedKeys, resultKeys)
		})
	}
}

func TestMarkerToInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval int64
		expected string
	}{
		{
			"positive interval",
			123,
			"interval-123",
		},
		{
			"zero interval",
			0,
			"interval-0",
		},
		{
			"negative interval",
			-456,
			"interval--456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := markerToInterval(tt.interval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseIntervalMarker(t *testing.T) {
	tests := []struct {
		name     string
		marker   string
		expected int64
		err      bool
	}{
		{
			"valid marker",
			"interval-123",
			123,
			false,
		},
		{
			"invalid marker",
			"invalid",
			0,
			true,
		},
		{
			"alpha interval",
			"interval-abc",
			0,
			true,
		},
		{
			"missing interval",
			"interval-",
			0,
			true,
		},
		{
			"blank interval",
			"interval--456",
			0,
			true,
		},
		{
			"extra parts",
			"interval-123-extra",
			123,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseIntervalMarker(tt.marker)
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestBox_loadOpenIntervals(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTTL(time.Hour))
	assert.NoError(t, err)

	// Set up the mock KVS with interval markers
	err = kvs.Set([]byte("interval-123"), []byte{}, 0)
	assert.NoError(t, err)
	err = kvs.Set([]byte("interval-456"), []byte{}, 0)
	assert.NoError(t, err)

	// Call loadOpenIntervals
	err = box.loadOpenIntervals()
	assert.NoError(t, err)

	// Verify that the openIntervals map is populated correctly
	expectedIntervals := map[int64]struct{}{
		123: {},
		456: {},
	}
	assert.Equal(t, expectedIntervals, box.openIntervals)
}

func TestIntervalToTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		count    int64
		expected time.Time
		ms       int64
	}{
		{
			"zero count",
			time.Minute,
			0,
			time.Unix(0, 0),
			0,
		},
		{
			"interval is 1 minute, count is 1",
			time.Minute,
			1,
			time.Unix(60, 0),
			60_000,
		},
		{
			"interval is 1 minute, count is 10",
			time.Minute,
			10,
			time.Unix(600, 0),
			600_000,
		},
		{
			"interval is 1 hour, count is 1",
			time.Hour,
			1,
			time.Unix(3600, 0),
			3_600_000,
		},
		{
			"interval is 1 hour, count is 10",
			time.Hour,
			10,
			time.Unix(0, 0).Add(time.Hour * 10),
			36_000_000,
		},
		{
			"interval is 10s",
			time.Second * 10,
			1,
			time.Unix(10, 0),
			10_000,
		},
		{
			"interval is 10s, count is 171987956",
			time.Second * 10,
			171987956,
			time.Unix(1719879560, 0),
			1_719_879_560_000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvs := NewMemoryKVS(nil)
			boxer, err := NewBoxer(WithKVS(kvs), WithInterval(tt.interval), WithIntervalCount(2), WithGrace(time.Millisecond*200))
			assert.NoError(t, err)
			result := boxer.TimeForInterval(tt.count)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.ms, result.UnixMilli())
		})
	}
}

func TestBox_GetClosedIntervals(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	kvs := NewMemoryKVS(timefunc)
	// Set up the mock KVS with interval markers
	assert.NoError(t, kvs.Set([]byte("interval-1230"), []byte{}, 0))
	assert.NoError(t, kvs.Set([]byte("interval-4560"), []byte{}, 0))
	// will be expired
	assert.NoError(t, kvs.Set([]byte("interval-1"), []byte{}, 0))
	assert.NoError(t, kvs.Set([]byte("interval-2"), []byte{}, 0))
	assert.NoError(t, kvs.Set([]byte("interval-997"), []byte{}, 0))

	box, err := NewBoxer(WithKVS(kvs), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTimeFunc(timefunc))
	assert.NoError(t, err)

	allIntervals, err := box.GetAllIntervals()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 997, 1230, 4560}, allIntervals, "all intervals did not check out")

	intervals, err := box.GetClosedIntervals(time.Unix(1000, 1000))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2}, intervals, "time 1000 did not return the expected intervals")

	box.timefunc = func() time.Time { return time.Unix(4560, 1000) }
	intervals, err = box.GetClosedIntervals(time.Unix(4560, 1000))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 997, 1230}, intervals, "time 4560 did not return the expected intervals")
}

func TestBox_IntervalsAndTime(t *testing.T) {
	b := &Boxer{
		interval:      time.Hour,
		intervalCount: 2,
	}

	interval := 1234
	ts := b.TimeForInterval(int64(interval))
	assert.Equal(t, time.Unix(0, 0).Add(time.Hour*time.Duration(interval)), ts)
	i2 := b.IntervalForTime(ts)
	assert.Equal(t, int64(interval), i2)
	ts2 := b.TimeForInterval(i2)
	assert.Equal(t, ts, ts2)
}
