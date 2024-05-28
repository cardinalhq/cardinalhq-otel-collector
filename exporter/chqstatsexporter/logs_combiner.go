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

package chqstatsexporter

import (
	"fmt"
	"time"

	"github.com/cespare/xxhash"
)

type LogStats struct {
	ServiceName string `json:"serviceName"`
	Fingerprint int64  `json:"fingerprint"`
	Filtered    bool   `json:"filtered"`
	WouldFilter bool   `json:"wouldFilter"`
	Count       int64  `json:"count"`
}

type LogStatsReport struct {
	SubmittedAt time.Time   `json:"submittedAt"`
	Stats       []*LogStats `json:"stats"`
}

func (l *LogStats) Key() uint64 {
	key := fmt.Sprintf("%s:%d:%t:%t", l.ServiceName, l.Fingerprint, l.Filtered, l.WouldFilter)
	return xxhash.Sum64String(key)
}

func (l *LogStats) Matches(other StatsObject) bool {
	otherLogStats, ok := other.(*LogStats)
	if !ok {
		return false
	}
	return l.ServiceName == otherLogStats.ServiceName &&
		l.Fingerprint == otherLogStats.Fingerprint &&
		l.Filtered == otherLogStats.Filtered &&
		l.WouldFilter == otherLogStats.WouldFilter
}

func (l *LogStats) Increment() {
	l.Count++
}
