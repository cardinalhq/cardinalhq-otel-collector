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

package translate

import (
	"maps"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func CalculateTID(extra map[string]string, rattr, sattr, iattr pcommon.Map, prefix string, environment Environment) int64 {
	tags := map[string]string{}
	maps.Copy(tags, extra)
	addKeys(rattr, "resource", tags)
	addKeys(sattr, "scope", tags)
	addKeys(iattr, prefix, tags)
	if environment != nil {
		for k, v := range environment.Tags() {
			tags["env."+k] = v
		}
	}
	return calculateTID(tags)
}

func addKeys(attr pcommon.Map, prefix string, tags map[string]string) {
	attr.Range(func(k string, v pcommon.Value) bool {
		if k[0] != '_' {
			tags[prefix+"."+k] = v.AsString()
		}
		return true
	})
}

func calculateTID(tags map[string]string) int64 {
	keys := []string{}
	for k := range tags {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	items := []string{}
	for _, k := range keys {
		v := tags[k]
		if v != "" {
			items = append(items, k, v)
		}
	}
	return int64(xxhash.Sum64String(strings.Join(items, ":")))
}
