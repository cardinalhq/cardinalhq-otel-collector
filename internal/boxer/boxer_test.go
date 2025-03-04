// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Copyright 2024-2025 CardinalHQ, Inc
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
			buffer := NewMemoryBuffer()
			box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(interval), WithIntervalCount(intervalCount), WithGrace(grace), WithTimeFunc(timefunc))
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
			buffer := NewMemoryBuffer()
			box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(interval), WithIntervalCount(intervalCount), WithGrace(grace), WithTimeFunc(timefunc))
			assert.NoError(t, err)
			result := box.intervalTooOld(timefunc(), tt.interval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_intervalNumber(t *testing.T) {
	buffer := NewMemoryBuffer()
	box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200))
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

func TestBox_Put(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	buffer := NewMemoryBuffer()
	box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTimeFunc(timefunc))
	assert.NoError(t, err)
	tests := []struct {
		name         string
		scope        string
		ts           time.Time
		value        []byte
		expectTooOld bool
	}{
		{
			"test1",
			"scope1",
			time.Unix(0, 0),
			[]byte("value1"),
			true,
		},
		{
			"test2",
			"scope2",
			time.Unix(1001, 0),
			[]byte("value2"),
			false,
		},
		{
			"test3",
			"scope3",
			time.Unix(1000, 0),
			[]byte("value3"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tooOld, err := box.Put(tt.scope, tt.ts, tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectTooOld, tooOld)
		})
	}
}

func TestBox_ForEach(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	tests := []struct {
		name     string
		records  []*BufferRecord
		tbox     int64
		scope    string
		expected []string
		err      error
	}{
		{
			"no items",
			[]*BufferRecord{},
			1000,
			"scope1",
			[]string{},
			NoSuchIntervalError,
		},
		{
			"one item different tbox",
			[]*BufferRecord{
				{
					Interval: 1001,
					Scope:    "scope1",
					Contents: []byte("value1"),
				},
			},
			1000,
			"scope1",
			[]string{},
			NoSuchIntervalError,
		},
		{
			"one item, this tbox",
			[]*BufferRecord{
				{
					Interval: 1000,
					Scope:    "scope1",
					Contents: []byte("value1"),
				},
			},
			1000,
			"scope1",
			[]string{"value1"},
			nil,
		},
		{
			"two items same scope",
			[]*BufferRecord{
				{
					Interval: 1000,
					Scope:    "scope1",
					Contents: []byte("value1"),
				},
				{
					Interval: 1000,
					Scope:    "scope1",
					Contents: []byte("value2"),
				},
			},
			1000,
			"scope1",
			[]string{"value1", "value2"},
			nil,
		},
		{
			"multiple scopes and tboxes",
			[]*BufferRecord{
				{
					Interval: 1000,
					Scope:    "scope1",
					Contents: []byte("value1"),
				},
				{
					Interval: 1001,
					Scope:    "scope1",
					Contents: []byte("value2"),
				},
				{
					Interval: 1000,
					Scope:    "scope2",
					Contents: []byte("value3"),
				},
				{
					Interval: 1001,
					Scope:    "scope2",
					Contents: []byte("value4"),
				},
			},
			1000,
			"scope1",
			[]string{"value1"},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := NewMemoryBuffer()
			box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTimeFunc(timefunc))
			assert.NoError(t, err)

			for _, record := range tt.records {
				err = buffer.Write(record)
				assert.NoError(t, err)
			}

			result := []string{}
			err = box.ForEach(tt.tbox, tt.scope, func(index, expected int, value []byte) (bool, error) {
				assert.Equal(t, len(result), index)
				assert.Equal(t, len(tt.expected), expected)
				result = append(result, string(value))
				return true, nil
			})
			assert.Equal(t, tt.err, err)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
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
			buffer := NewMemoryBuffer()
			box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(tt.interval), WithIntervalCount(2), WithGrace(time.Millisecond*200))
			assert.NoError(t, err)
			result := box.TimeForInterval(tt.count)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.ms, result.UnixMilli())
		})
	}
}

func TestBox_GetClosedIntervals(t *testing.T) {
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	buffer := NewMemoryBuffer()

	assert.NoError(t, buffer.Write(&BufferRecord{
		Interval: 1,
		Scope:    "scope1",
	}))
	assert.NoError(t, buffer.Write(&BufferRecord{
		Interval: 2,
		Scope:    "scope1",
	}))
	assert.NoError(t, buffer.Write(&BufferRecord{
		Interval: 997,
		Scope:    "scope1",
	}))
	assert.NoError(t, buffer.Write(&BufferRecord{
		Interval: 1230,
		Scope:    "scope1",
	}))
	assert.NoError(t, buffer.Write(&BufferRecord{
		Interval: 4560,
		Scope:    "scope1",
	}))

	box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200), WithTimeFunc(timefunc))
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

func TestBox_CloseInterval(t *testing.T) {
	buffer := NewMemoryBuffer()
	box, err := NewBoxer(WithBufferStorage(buffer), WithInterval(time.Second), WithIntervalCount(2), WithGrace(time.Millisecond*200))
	assert.NoError(t, err)

	records := []*BufferRecord{
		{
			Interval: 1000,
			Scope:    "scope1",
			Contents: []byte("value1"),
		},
		{
			Interval: 1001,
			Scope:    "scope1",
			Contents: []byte("value2"),
		},
		{
			Interval: 1001,
			Scope:    "scope2",
			Contents: []byte("value3"),
		},
		{
			Interval: 1001,
			Scope:    "scope2",
			Contents: []byte("value4"),
		},
	}

	for _, record := range records {
		err = buffer.Write(record)
		assert.NoError(t, err)
	}

	err = box.CloseInterval(1000)
	assert.NoError(t, err)

	// Check that the interval is closed
	intervals, err := box.GetAllIntervals()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{1000, 1001}, intervals)
}
