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
	"fmt"
	"sort"

	"github.com/cespare/xxhash"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

func (l *LogStats) Key() uint64 {
	key := fmt.Sprintf("%s:%d:%d:%s", l.ServiceName, l.Fingerprint, int32(l.Phase), l.VendorId)
	return xxhash.Sum64String(key)
}

func (l *LogStats) Matches(other stats.StatsObject) bool {
	otherLogStats, ok := other.(*LogStats)
	if !ok {
		return false
	}
	return l.ServiceName == otherLogStats.ServiceName &&
		l.Fingerprint == otherLogStats.Fingerprint &&
		l.Phase == otherLogStats.Phase &&
		l.VendorId == otherLogStats.VendorId
}

func (l *LogStats) Increment(_ string, count int, size int64) error {
	l.Count += int64(count)
	l.LogSize += size
	return nil
}

func (l *LogStats) Initialize() error {
	return nil
}

func (l *SpanStats) Key() uint64 {
	// Create the initial key with ServiceName, Fingerprint, Phase, and VendorId
	key := fmt.Sprintf("%s:%d:%d:%s", l.ServiceName, l.Fingerprint, l.Phase, l.VendorId)

	// Collect keys from the tags map and sort them to ensure consistent order
	var sortedKeys []string
	for k := range l.Tags {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Append sorted key-value pairs to the key
	for _, k := range sortedKeys {
		key += fmt.Sprintf(":%s=%s", k, l.Tags[k])
	}

	// Return the xxhash of the resulting key string
	return xxhash.Sum64String(key)
}

func (l *SpanStats) Matches(other stats.StatsObject) bool {
	otherSpanStats, ok := other.(*SpanStats)
	if !ok {
		return false
	}
	return l.Key() == otherSpanStats.Key()
}

func (l *SpanStats) Increment(_ string, count int, size int64) error {
	l.Count += int64(count)
	l.SpanSize += size
	return nil
}

func (l *SpanStats) Initialize() error {
	return nil
}
