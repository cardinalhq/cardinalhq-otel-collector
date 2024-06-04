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
	"github.com/apache/datasketches-go/hll"
	"github.com/cespare/xxhash"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

type MetricStat struct {
	MetricName  string
	TagName     string
	ServiceName string
	Phase       chqpb.Phase
	HLL         hll.HllSketch
}

func (m *MetricStat) Key() uint64 {
	return xxhash.Sum64String(m.MetricName + ":" + m.TagName)
}

func (m *MetricStat) Increment(tag string, count int, _ int64) error {
	if m.HLL == nil {
		hll, err := hll.NewHllSketchWithDefault()
		if err != nil {
			return err
		}
		m.HLL = hll
	}
	for i := 0; i < count; i++ {
		err := m.HLL.UpdateString(tag)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MetricStat) Matches(other stats.StatsObject) bool {
	o, ok := other.(*MetricStat)
	if !ok {
		return false
	}
	return m.MetricName == o.MetricName &&
		m.TagName == o.TagName &&
		m.ServiceName == o.ServiceName &&
		m.Phase == o.Phase
}
