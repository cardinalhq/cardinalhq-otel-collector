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
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSplitTag(t *testing.T) {
	tests := []struct {
		name          string
		tag           string
		expectedScope string
		expectedName  string
	}{
		{"scope.name", "scope.name", "scope", "name"},
		{"invalidTag", "invalidtag", "", ""},
		{".foo", ".foo", "", ""},
		{"foo.", "foo.", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope, name := splitTag(tt.tag)
			if scope != tt.expectedScope {
				t.Errorf("Expected scope %s, but got %s", tt.expectedScope, scope)
			}
			if name != tt.expectedName {
				t.Errorf("Expected name %s, but got %s", tt.expectedName, name)
			}
		})
	}
}

func TestRemoveTags(t *testing.T) {
	type args struct {
		attrs map[string]string
		tags  []string
	}
	tests := []struct {
		name     string
		args     args
		expected map[string]string
	}{
		{
			name: "remove all tags",
			args: args{
				attrs: map[string]string{
					"scope1.name1": "value1",
					"scope1.name2": "value2",
					"scope2.name3": "value3",
				},
				tags: []string{"scope1.name1", "scope1.name2", "scope2.name3"},
			},
			expected: map[string]string{},
		},
		{
			name: "remove some tags",
			args: args{
				attrs: map[string]string{
					"scope1.name1": "value1",
					"scope1.name2": "value2",
					"scope2.name3": "value3",
				},
				tags: []string{"scope1.name1", "scope2.name3"},
			},
			expected: map[string]string{
				"scope1.name2": "value2",
			},
		},
		{
			name: "remove no tags",
			args: args{
				attrs: map[string]string{
					"scope1.name1": "value1",
					"scope1.name2": "value2",
					"scope2.name3": "value3",
				},
				tags: []string{},
			},
			expected: map[string]string{
				"scope1.name1": "value1",
				"scope1.name2": "value2",
				"scope2.name3": "value3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RemoveTags(tt.args.attrs, tt.args.tags)
			assert.Equal(t, tt.expected, tt.args.attrs)
		})
	}
}

func TestKeepTags(t *testing.T) {
	type args struct {
		attrs map[string]string
		tags  []string
	}
	tests := []struct {
		name     string
		args     args
		expected map[string]string
	}{
		{
			name: "keep all tags",
			args: args{
				attrs: map[string]string{
					"scope1.name1": "value1",
					"scope1.name2": "value2",
					"scope2.name3": "value3",
				},
				tags: []string{"scope1.name1", "scope1.name2", "scope2.name3"},
			},
			expected: map[string]string{
				"scope1.name1": "value1",
				"scope1.name2": "value2",
				"scope2.name3": "value3",
			},
		},
		{
			name: "keep some tags",
			args: args{
				attrs: map[string]string{
					"scope1.name1": "value1",
					"scope1.name2": "value2",
					"scope2.name3": "value3",
				},
				tags: []string{"scope1.name1", "scope2.name3"},
			},
			expected: map[string]string{
				"scope1.name1": "value1",
				"scope2.name3": "value3",
			},
		},
		{
			name: "keep no tags",
			args: args{
				attrs: map[string]string{
					"scope1.name1": "value1",
					"scope1.name2": "value2",
					"scope2.name3": "value3",
				},
				tags: []string{},
			},
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			KeepTags(tt.args.attrs, tt.args.tags)
			assert.Equal(t, tt.expected, tt.args.attrs)
		})
	}
}

func TestAttrsToMap(t *testing.T) {
	attrs := map[string]pcommon.Map{
		"scope1": pcommon.NewMap(),
		"scope2": pcommon.NewMap(),
	}

	attrs["scope1"].PutStr("name1", "value1")
	attrs["scope1"].PutInt("name2", 123)
	attrs["scope2"].PutBool("name3", true)

	expected := map[string]string{
		"scope1.name1": "value1",
		"scope1.name2": "123",
		"scope2.name3": "true",
	}

	result := attrsToMap(attrs)

	if len(result) != len(expected) {
		t.Errorf("Expected %d attributes, but got %d", len(expected), len(result))
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("Expected value %s for attribute %s, but got %s", v, k, result[k])
		}
	}
}

func TestTimebox(t *testing.T) {
	tests := []struct {
		name     string
		t        time.Time
		interval int64
		want     int64
	}{
		{
			name:     "time within interval",
			t:        time.UnixMilli(1641024001231),
			interval: 1000,
			want:     1641024001000,
		},
		{
			name:     "time at interval boundary start",
			t:        time.UnixMilli(1641024000000),
			interval: 1000,
			want:     1641024000000,
		},
		{
			name:     "time at interval boundary end",
			t:        time.UnixMilli(1641024000999),
			interval: 1000,
			want:     1641024000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := timebox(tt.t, tt.interval)
			assert.Equal(t, tt.want, actual)
		})
	}
}

func TestConfigure(t *testing.T) {
	m := NewMetricAggregatorImpl[float64](10, nil)
	rules := []AggregatorConfig{
		{
			Id:         "rule1",
			MetricName: "metric1",
		},
	}

	assert.Empty(t, m.rules)
	m.Configure(rules)
	assert.Equal(t, rules, m.rules)
}

func TestMatchAndAdd(t *testing.T) {
	conf := []AggregatorConfig{
		{
			Id:         "rule1",
			MetricName: "metric1",
			Scope: map[string]string{
				"resource.name": "resource1",
			},
			Tags: []string{
				"resource.name",
				"instrumentation.name",
			},
			TagAction: "keep",
		},
	}
	m := NewMetricAggregatorImpl[float64](10, conf)

	rattr := pcommon.NewMap()
	rattr.PutStr("name", "resource1")

	iattr := pcommon.NewMap()
	iattr.PutStr("name", "instrumentation1")

	mattr := pcommon.NewMap()
	mattr.PutStr("name", "metric1")

	log.Printf("rattr: %v", rattr.AsRaw())

	buckets := []float64{1}
	values := []float64{10.0}
	aggregationType := AggregationTypeSum
	name := "metric1"

	expectedTags := map[string]string{
		"resource.name":        "resource1",
		"instrumentation.name": "instrumentation1",
	}

	expectedId := "rule1"

	ttime := time.UnixMilli(1641024001231)
	tbox := timebox(ttime, 10)

	actualId, err := m.MatchAndAdd(&ttime, buckets, values, aggregationType, name, rattr, iattr, mattr)
	assert.Nil(t, err)

	assert.Equal(t, expectedId, actualId)
	assert.Equal(t, 1, len(m.sets))
	assert.Equal(t, 1, len(m.sets[tbox].Aggregations))
	// we don't know what the actual entry is, so we will pull out
	// whatever is there.
	for _, v := range m.sets[tbox].Aggregations {
		assert.Equal(t, values, v.Value())
		assert.Equal(t, aggregationType, v.AggregationType())
		assert.Equal(t, expectedTags, v.Tags())
		assert.Equal(t, uint64(1), v.Count())
	}

	actualId, err = m.MatchAndAdd(&ttime, buckets, values, aggregationType, "bob", rattr, iattr, mattr)
	assert.Nil(t, err)
	assert.Equal(t, "", actualId)
	// should be no changes
	assert.Equal(t, 1, len(m.sets))
	assert.Equal(t, 1, len(m.sets[tbox].Aggregations))
	// we don't know what the actual entry is, so we will pull out
	// whatever is there.
	for _, v := range m.sets[tbox].Aggregations {
		assert.Equal(t, values, v.Value())
		assert.Equal(t, aggregationType, v.AggregationType())
		assert.Equal(t, expectedTags, v.Tags())
		assert.Equal(t, uint64(1), v.Count())
	}

}

func TestMatchscopeMap(t *testing.T) {
	tests := []struct {
		name   string
		scope  map[string]string
		attrs  map[string]string
		result bool
	}{
		{
			name: "matching scope and attrs",
			scope: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			attrs: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			result: true,
		},
		{
			name: "non-matching scope and attrs",
			scope: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			attrs: map[string]string{
				"key1": "value1",
				"key2": "value3",
			},
			result: false,
		},
		{
			name:   "empty scope and attrs",
			scope:  map[string]string{},
			attrs:  map[string]string{},
			result: true,
		},
		{
			name:  "empty scope and non-empty attrs",
			scope: map[string]string{},
			attrs: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			result: true,
		},
		{
			name: "non-empty scope and empty attrs",
			scope: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			attrs:  map[string]string{},
			result: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := matchscopeMap(tt.scope, tt.attrs)
			assert.Equal(t, tt.result, actual)
		})
	}
}
