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

package chqpb

import (
	"testing"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
	"github.com/stretchr/testify/assert"
)

type mockStatsObject struct{}

func (m *mockStatsObject) Key() uint64 { return 0 }

func (m *mockStatsObject) Matches(stats.StatsObject) bool { return false }

func (m *mockStatsObject) Increment() {}

func TestLogStats_Key(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		logStats *LogStats
		want     uint64
	}{
		{
			name: "alice 1234",
			logStats: &LogStats{
				ServiceName: "alice",
				Fingerprint: 1234,
				WasFiltered: false,
				WouldFilter: false,
			},
			want: 0x497f0c2d921fe053,
		},
		{
			name: "bob 5678",
			logStats: &LogStats{
				ServiceName: "bob",
				Fingerprint: 5678,
				WasFiltered: true,
				WouldFilter: true,
			},
			want: 0x4752aee96157ea5b,
		},
		{
			name: "bob 567",
			logStats: &LogStats{
				ServiceName: "bob",
				Fingerprint: 567,
				WasFiltered: true,
				WouldFilter: true,
			},
			want: 0x83a875d2990a8ce5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.logStats.Key()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLogStats_Matches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		logStats   *LogStats
		other      stats.StatsObject
		wantResult bool
	}{
		{
			name: "matching log stats",
			logStats: &LogStats{
				ServiceName: "alice",
				Fingerprint: 1234,
				WasFiltered: false,
				WouldFilter: false,
			},
			other: &LogStats{
				ServiceName: "alice",
				Fingerprint: 1234,
				WasFiltered: false,
				WouldFilter: false,
			},
			wantResult: true,
		},
		{
			name: "non-matching log stats",
			logStats: &LogStats{
				ServiceName: "alice",
				Fingerprint: 1234,
				WasFiltered: false,
				WouldFilter: false,
			},
			other: &LogStats{
				ServiceName: "bob",
				Fingerprint: 5678,
				WasFiltered: true,
				WouldFilter: true,
			},
			wantResult: false,
		},
		{
			name: "non-log stats object",
			logStats: &LogStats{
				ServiceName: "alice",
				Fingerprint: 1234,
				WasFiltered: false,
				WouldFilter: false,
			},
			other:      &mockStatsObject{},
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult := tt.logStats.Matches(tt.other)
			assert.Equal(t, tt.wantResult, gotResult)
		})
	}
}

func TestLogStats_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		logStats  *LogStats
		wantCount int64
	}{
		{
			name: "increment count",
			logStats: &LogStats{
				ServiceName: "alice",
				Fingerprint: 1234,
				WasFiltered: false,
				WouldFilter: false,
				Count:       0,
			},
			wantCount: 1,
		},
		{
			name: "increment count multiple times",
			logStats: &LogStats{
				ServiceName: "bob",
				Fingerprint: 5678,
				WasFiltered: true,
				WouldFilter: true,
				Count:       2,
			},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.logStats.Increment()
			assert.Equal(t, tt.wantCount, tt.logStats.Count)
		})
	}
}
