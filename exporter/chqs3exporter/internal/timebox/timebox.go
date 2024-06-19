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

type Timebox interface {
	Append(item map[string]any)
	ShouldClose(now int64) bool
	Items() []map[string]any
}

func CalculateInterval(t int64, interval int64) int64 {
	return t - (t % interval)
}

type TimeboxImpl struct {
	// Interval is the "data time" of the start of this timebox.
	Interval int64
	// Expiry is the "data time" to close this timebox.  It will be calculated
	// as the start of the timebox + Interval + GracePeriod.
	Expiry int64
	// Items is the list of items in this timebox.
	items []map[string]any
}

func NewTimeboxImpl(interval int64, expiry int64) Timebox {
	return &TimeboxImpl{
		Interval: interval,
		Expiry:   expiry,
		items:    []map[string]any{},
	}
}

func (t *TimeboxImpl) Append(item map[string]any) {
	t.items = append(t.items, item)
}

func (t *TimeboxImpl) ShouldClose(now int64) bool {
	return now > t.Expiry
}

func (t *TimeboxImpl) Items() []map[string]any {
	return t.items
}
