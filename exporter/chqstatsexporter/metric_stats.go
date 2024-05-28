package chqstatsexporter

import (
	"github.com/apache/datasketches-go/hll"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
	"github.com/cespare/xxhash"
)

type MetricStat struct {
	Name    string
	TagName string
	HLL     hll.HllSketch
}

func (m *MetricStat) Key() uint64 {
	return xxhash.Sum64String(m.Name + ":" + m.TagName)
}

func (m *MetricStat) Increment(tag string, count int) error {
	if m.HLL == nil {
		hll, err := hll.NewHllSketchWithDefault()
		if err != nil {
			return err
		}
		m.HLL = hll
	}
	return m.HLL.UpdateString(tag)
}

func (m *MetricStat) Matches(other stats.StatsObject) bool {
	o, ok := other.(*MetricStat)
	if !ok {
		return false
	}
	return m.Name == o.Name && m.TagName == o.TagName
}
