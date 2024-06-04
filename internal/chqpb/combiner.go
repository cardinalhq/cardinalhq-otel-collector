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

	"github.com/cespare/xxhash"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

func (l *LogStats) Key() uint64 {
	key := fmt.Sprintf("%s:%d:%d", l.ServiceName, l.Fingerprint, int32(l.Phase))
	return xxhash.Sum64String(key)
}

func (l *LogStats) Matches(other stats.StatsObject) bool {
	otherLogStats, ok := other.(*LogStats)
	if !ok {
		return false
	}
	return l.ServiceName == otherLogStats.ServiceName &&
		l.Fingerprint == otherLogStats.Fingerprint &&
		l.Phase == otherLogStats.Phase
}

func (l *LogStats) Increment(_ string, count int, size int64) error {
	l.Count += int64(count)
	l.LogSize += size
	return nil
}

func (l *LogStats) Initialize() error {
	return nil
}
