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

package chqs3exporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClosed(t *testing.T) {
	type args struct {
		now      int64
		ts       int64
		interval int64
		grace    int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "now is before ts",
			args: args{
				now:      0,
				ts:       100,
				interval: 100,
				grace:    0,
			},
			want: false,
		},
		{
			name: "now is after ts, grace is zero",
			args: args{
				now:      100,
				ts:       0,
				interval: 100,
				grace:    0,
			},
			want: true,
		},
		{
			name: "now is after ts, grace is non-zero",
			args: args{
				now:      100,
				ts:       0,
				interval: 100,
				grace:    10,
			},
			want: false,
		},
		{
			name: "now is after ts + interval + grace",
			args: args{
				now:      200,
				ts:       0,
				interval: 100,
				grace:    10,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := closed(tt.args.now, tt.args.ts, tt.args.interval, tt.args.grace)
			assert.Equal(t, tt.want, got)
		})
	}
}
