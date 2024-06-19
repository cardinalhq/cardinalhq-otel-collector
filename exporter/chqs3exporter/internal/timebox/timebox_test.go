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

func TestCalculateInterval(t *testing.T) {
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
			got := CalculateInterval(tt.t, tt.interval)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewTimebox(t *testing.T) {
	interval := CalculateInterval(123456789000, 10_000)
	expiry := interval + 10_000

	timebox := NewTimeboxImpl(interval, expiry)
	tb := timebox.(*TimeboxImpl)

	assert.Equal(t, interval, tb.Interval)
	assert.Equal(t, expiry, tb.Expiry)
	assert.Empty(t, timebox.Items())
}

func TestAppend(t *testing.T) {
	now := time.Unix(1234567890, 0)
	interval := CalculateInterval(now.UnixMilli(), 10_000)
	expiry := interval + 10_000
	timebox := NewTimeboxImpl(interval, expiry)

	item1 := map[string]any{"key1": "value1"}
	timebox.Append(item1)
	assert.ElementsMatch(t, []map[string]any{item1}, timebox.Items())

	item2 := map[string]any{"key2": "value2"}
	timebox.Append(item2)
	assert.ElementsMatch(t, []map[string]any{item1, item2}, timebox.Items())
}

func TestShouldClose(t *testing.T) {
	tests := []struct {
		name     string
		interval int64
		now      int64
		expiry   int64
		want     bool
	}{
		{"now before expiry", 1234567890, 1234567889, 1234567890, false},
		{"now equal to expiry", 1234567890, 1234567890, 1234567890, false},
		{"now after expiry", 1234567890, 1234567891, 1234567890, true},
		{"interval is very different than now", 40, 1234567889, 1234567890, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timebox := NewTimeboxImpl(tt.interval, tt.expiry)
			got := timebox.ShouldClose(tt.now)
			assert.Equal(t, tt.want, got)
		})
	}
}
