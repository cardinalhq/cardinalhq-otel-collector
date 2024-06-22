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

package sampler

import (
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type MetricAggregator[T int64 | float64] interface {
	Emit(now time.Time) map[int64]*AggregationSet[T]
	Configure(rules []AggregatorConfigV1, vendor string)
	MatchAndAdd(t *time.Time, buckets []T, value []T, aggregationType AggregationType, name string, metadata map[string]string, rattr pcommon.Map, iattr pcommon.Map, mattr pcommon.Map) (*AggregatorConfigV1, error)
	HasRules() bool
}

type MetricAggregatorImpl[T int64 | float64] struct {
	sets      map[int64]*AggregationSet[T]
	setsLock  sync.Mutex
	rules     []AggregatorConfigV1
	rulesLock sync.RWMutex
	interval  int64
}

var _ MetricAggregator[int64] = (*MetricAggregatorImpl[int64])(nil)

func NewMetricAggregatorImpl[T int64 | float64](interval int64, rules []AggregatorConfigV1) *MetricAggregatorImpl[T] {
	return &MetricAggregatorImpl[T]{
		sets:     map[int64]*AggregationSet[T]{},
		interval: interval,
		rules:    rules,
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

func (m *MetricAggregatorImpl[T]) Configure(rules []AggregatorConfigV1, vendor string) {
	m.rulesLock.Lock()
	defer m.rulesLock.Unlock()
	newrules := []AggregatorConfigV1{}
	for _, rule := range rules {
		if rule.Vendor == vendor {
			newrules = append(newrules, rule)
		}
	}
	m.rules = newrules
}

func (m *MetricAggregatorImpl[T]) HasRules() bool {
	m.rulesLock.RLock()
	defer m.rulesLock.RUnlock()
	return len(m.rules) > 0
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
) (*AggregatorConfigV1, error) {
	m.rulesLock.RLock()
	defer m.rulesLock.RUnlock()
	t = nowtime(t)
	for _, rule := range m.rules {
		if rule.MetricName != "" && rule.MetricName != name {
			continue
		}
		attrs := attrsToMap(map[string]pcommon.Map{
			"resource":        rattr,
			"instrumentation": iattr,
			"metric":          mattr,
		})
		for k, v := range metadata {
			attrs["metadata."+k] = v
		}
		if matchscopeMap(rule.Scope, attrs) {
			if len(rule.Tags) > 0 {
				if rule.TagAction == "keep" {
					KeepTags(attrs, rule.Tags)
				} else {
					RemoveTags(attrs, rule.Tags)
				}
			}
			err := m.add(*t, name, buckets, values, aggregationType, attrs)
			return &rule, err
		}
	}
	return nil, nil
}

func matchscopeMap(scope map[string]string, attrs map[string]string) bool {
	for k, v := range scope {
		if attrs[k] != v {
			return false
		}
	}
	return true
}

func attrsToMap(attrs map[string]pcommon.Map) map[string]string {
	ret := map[string]string{}
	for scope, attr := range attrs {
		attr.Range(func(k string, v pcommon.Value) bool {
			ret[scope+"."+k] = v.AsString()
			return true
		})
	}
	return ret
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
