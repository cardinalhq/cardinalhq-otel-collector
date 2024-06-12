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

import "time"

func CalculateInterval(t int64, interval int64) int64 {
	return t - (t % interval)
}

type Timebox struct {
	// Interval is the "data time" of the start of this timebox.
	Interval int64
	// Expiry is the wall clock time to close this timebox.  It will be calculated
	// as $now + Interval + GracePeriod.
	Expiry time.Time
	// Items is the list of items in this timebox.
	Items []map[string]any
}

func NewTimebox(interval int64, expiry time.Time) *Timebox {
	return &Timebox{
		Interval: interval,
		Expiry:   expiry,
		Items:    []map[string]any{},
	}
}

func (t *Timebox) Append(item map[string]any) {
	t.Items = append(t.Items, item)
}

func (t *Timebox) ShouldClose(now time.Time) bool {
	return now.After(t.Expiry)
}
