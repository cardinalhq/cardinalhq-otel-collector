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

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"
)

func FingerprintTags(tags map[string]string) uint64 {
	keys := maps.Keys(tags)
	slices.Sort(keys)
	values := make([]string, 0, len(keys))
	for _, k := range keys {
		values = append(values, k+"="+tags[k])
	}
	return hashTagValues(values)
}

func hashTagValues(values []string) uint64 {
	return xxhash.Sum64String(strings.Join(values, ","))
}

func matchscope(scope map[string]string, attrs map[string]pcommon.Map) bool {
	if len(scope) == 0 {
		return true
	}
	for k, v := range scope {
		parts := strings.SplitN(k, ".", 2)
		if len(parts) != 2 {
			return false
		}
		sattr, ok := attrs[parts[0]]
		if !ok {
			return false
		}
		if !matchTag(parts[1], sattr, v) {
			return false
		}
	}
	return true
}

func matchTag(key string, tags pcommon.Map, value string) bool {
	if v, ok := tags.Get(key); ok {
		return v.AsString() == value
	}
	return false
}
