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

package ottl

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type MetricAggregator[T int64 | float64] interface {
	Emit(now time.Time) map[int64]*AggregationSet[T]
	MatchAndAdd(t *time.Time, buckets []T, value []T, aggregationType AggregationType, name string, metadata map[string]string, rattr pcommon.Map, iattr pcommon.Map, mattr pcommon.Map) (bool, error)
}

type MetricAggregatorImpl[T int64 | float64] struct {
	sets      map[int64]*AggregationSet[T]
	setsLock  sync.Mutex
	rulesLock sync.RWMutex
	interval  int64
}

var _ MetricAggregator[int64] = (*MetricAggregatorImpl[int64])(nil)

func NewMetricAggregatorImpl[T int64 | float64](interval int64) *MetricAggregatorImpl[T] {
	return &MetricAggregatorImpl[T]{
		sets:     map[int64]*AggregationSet[T]{},
		interval: interval,
	}
}

func (m *MetricAggregatorImpl[T]) Emit(now time.Time) map[int64]*AggregationSet[T] {
	ret := map[int64]*AggregationSet[T]{}
	nnow := now.UTC().UnixMilli()
	// TODO add grace rather than just emitting previous interval
	interval := nnow - (nnow % m.interval) - m.interval
	m.setsLock.Lock()
	defer m.setsLock.Unlock()
	for k, v := range m.sets {
		if k < interval {
			ret[k] = v
			delete(m.sets, k)
		}
	}
	return ret
}

func timebox(t time.Time, interval int64) int64 {
	n := t.UTC().UnixMilli()
	return n - (n % interval)
}

func (m *MetricAggregatorImpl[T]) add(t time.Time, name string, buckets []T, values []T, aggregationType AggregationType, tags map[string]string) error {
	interval := timebox(t, m.interval)
	m.setsLock.Lock()
	defer m.setsLock.Unlock()
	set, ok := m.sets[interval]
	if !ok {
		set = NewAggregationSet[T](interval, m.interval)
		m.sets[interval] = set
	}
	return set.Add(name, buckets, values, aggregationType, tags)
}

func nowtime(t *time.Time) *time.Time {
	if t == nil {
		tt := time.Now()
		return &tt
	}
	return t
}

func (m *MetricAggregatorImpl[T]) MatchAndAdd(
	t *time.Time,
	buckets []T,
	values []T,
	aggregationType AggregationType,
	name string,
	metadata map[string]string,
	rattr pcommon.Map,
	iattr pcommon.Map,
	mattr pcommon.Map,
) (bool, error) {
	m.rulesLock.RLock()
	defer m.rulesLock.RUnlock()
	t = nowtime(t)
	attrs, shouldAggregate := attrsToMap(map[string]pcommon.Map{
		"resource":        rattr,
		"instrumentation": iattr,
		"metric":          mattr,
	})
	for k, v := range metadata {
		attrs["metadata."+k] = v
	}
	if shouldAggregate {
		err := m.add(*t, name, buckets, values, aggregationType, attrs)
		return true, err
	}
	return false, nil
}

func attrsToMap(attrs map[string]pcommon.Map) (map[string]string, bool) {
	ret := map[string]string{}
	var shouldAggregate bool
	for scope, attr := range attrs {
		attr.Range(func(k string, v pcommon.Value) bool {
			shouldAggregate = shouldAggregate || k == translate.CardinalFieldAggregate && v.Bool()
			ret[scope+"."+k] = v.AsString()
			return true
		})
	}
	return ret, shouldAggregate
}

func RemoveTags(attrs map[string]string, tags []string) {
	for _, tag := range tags {
		delete(attrs, tag)
	}
}

func KeepTags(attrs map[string]string, tags []string) {
	for k := range attrs {
		if !slices.Contains(tags, k) {
			delete(attrs, k)
		}
	}
}

func SplitTag(tag string) (scope string, name string) {
	parts := strings.SplitN(tag, ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	if parts[0] == "" || parts[1] == "" {
		return "", ""
	}
	return parts[0], parts[1]
}
