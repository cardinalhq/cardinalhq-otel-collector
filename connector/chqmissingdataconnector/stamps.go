// Copyright 2024-2025 CardinalHQ, Inc
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

package chqmissingdataconnector

import (
	"slices"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type stamp struct {
	LastSeen            time.Time
	ResourceAttributes  pcommon.Map
	DatapointAttributes pcommon.Map
}

func newStamp(rattrs pcommon.Map, dattrs pcommon.Map, t time.Time) *stamp {
	s := &stamp{
		LastSeen:            t,
		ResourceAttributes:  pcommon.NewMap(),
		DatapointAttributes: pcommon.NewMap(),
	}
	rattrs.CopyTo(s.ResourceAttributes)
	dattrs.CopyTo(s.DatapointAttributes)
	return s
}

func (s *stamp) String() string {
	sb := strings.Builder{}
	sb.WriteString("Stamp{")

	sb.WriteString("ResourceAttributes: {")
	sb.WriteString(attributesToSortedString(s.ResourceAttributes))
	sb.WriteString("}, ")

	sb.WriteString("DatapointAttributes: {")
	sb.WriteString(attributesToSortedString(s.DatapointAttributes))
	sb.WriteString("}")

	sb.WriteString("}")

	return sb.String()
}

func attributesToSortedString(attrs pcommon.Map) string {
	items := []string{}

	attrs.Range(func(k string, v pcommon.Value) bool {
		items = append(items, k+"="+v.AsString())
		return true
	})
	slices.Sort(items)

	return strings.Join(items, ", ")
}

func (s *stamp) touch(t time.Time) {
	s.LastSeen = t
}

func (s *stamp) isExpired(t time.Time, ttl time.Duration) bool {
	return t.Sub(s.LastSeen) > ttl
}

func (s *stamp) hash() uint64 {
	return hashMetric(s.ResourceAttributes, s.DatapointAttributes)
}

func hashMetric(resourceAttributes pcommon.Map, dpattrs pcommon.Map) uint64 {
	xh := xxhash.New()
	hashAttributesWithHasher(resourceAttributes, xh)
	hashAttributesWithHasher(dpattrs, xh)
	return xh.Sum64()
}

func hashAttributes(attrs pcommon.Map) uint64 {
	xh := xxhash.New()
	hashAttributesWithHasher(attrs, xh)
	return xh.Sum64()
}

func hashAttributesWithHasher(attrs pcommon.Map, xh *xxhash.Digest) {
	keys := []string{}
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	slices.Sort(keys)

	for _, k := range keys {
		_, _ = xh.WriteString(k)
		v, _ := attrs.Get(k)
		_, _ = xh.WriteString(v.AsString())
	}
}
