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

package chqstatsprocessor

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
	VendorID    string
	Count       int64
	HLL         hll.HllSketch
	Tags        map[string]string
}

var _ stats.StatsObject = (*MetricStat)(nil)

func (m *MetricStat) Key() uint64 {
	key := m.MetricName + ":" + m.TagName + ":" + m.ServiceName + ":" + m.Phase.String() + ":" + m.VendorID
	key = chqpb.AppendTagsToKey(m.Tags, key)
	return xxhash.Sum64String(key)
}

func (m *MetricStat) Increment(tag string, count int, _ int64) error {
	if err := m.HLL.UpdateString(tag); err != nil {
		return err
	}
	m.Count += int64(count)
	return nil
}

func (m *MetricStat) Initialize() error {
	hll, err := hll.NewHllSketchWithDefault()
	if err != nil {
		return err
	}
	m.HLL = hll
	return nil
}

func (m *MetricStat) Matches(other stats.StatsObject) bool {
	_, ok := other.(*MetricStat)
	if !ok {
		return false
	}
	return m.Key() == other.Key()
}
