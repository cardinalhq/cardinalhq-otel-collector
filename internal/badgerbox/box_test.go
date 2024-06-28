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

package badgerbox

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
			box := NewBox(nil, interval, intervalCount, grace, NoTTL, timefunc)
			result := box.tooOld(tt.ts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_intervalNumber(t *testing.T) {
	box := NewBox(nil, time.Second, 2, time.Millisecond*200, time.Hour, nil)
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
			result := box.intervalNumber(tt.ts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBox_generatePrefix(t *testing.T) {
	box := NewBox(nil, time.Second, 2, time.Millisecond*200, time.Hour, nil)
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
	box := NewBox(nil, time.Second, 2, time.Millisecond*200, time.Hour, nil)
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

func TestBox_Put(t *testing.T) {
	kvs := NewMemoryKVS(nil)
	timefunc := func() time.Time { return time.Unix(1000, 1000) }
	box := NewBox(kvs, time.Second, 2, time.Millisecond*200, time.Hour, timefunc)
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
