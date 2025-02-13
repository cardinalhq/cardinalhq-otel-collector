package chqmissingdataconnector

import (
	"slices"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type Stamp struct {
	LastSeen            time.Time
	MetricName          string
	ResourceAttributes  pcommon.Map
	DatapointAttributes pcommon.Map
}

func NewStamp(metricName string, rattrs pcommon.Map, t time.Time) *Stamp {
	s := &Stamp{
		LastSeen:           t,
		MetricName:         metricName,
		ResourceAttributes: pcommon.NewMap(),
	}
	rattrs.CopyTo(s.ResourceAttributes)
	return s
}

func (s *Stamp) Touch(t time.Time) {
	s.LastSeen = t
}

func (s *Stamp) IsExpired(t time.Time, ttl time.Duration) bool {
	return t.Sub(s.LastSeen) > ttl
}

func (s *Stamp) Hash() uint64 {
	return makeStampHash(s.MetricName, s.ResourceAttributes)
}

func makeStampHash(metricName string, resourceAttributes pcommon.Map) uint64 {
	xh := xxhash.New()
	xh.WriteString(metricName)

	keys := []string{}
	resourceAttributes.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})

	slices.Sort(keys)
	for _, k := range keys {
		xh.WriteString(k)
		v, _ := resourceAttributes.Get(k)
		xh.WriteString(v.AsString())
	}

	return xh.Sum64()
}
