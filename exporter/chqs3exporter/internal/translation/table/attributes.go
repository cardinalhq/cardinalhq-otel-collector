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

package table

import "go.opentelemetry.io/collector/pdata/pcommon"

func addAttributes(m map[string]any, attrs pcommon.Map, prefix string) {
	attrs.Range(func(k string, v pcommon.Value) bool {
		name := k
		targetName := ""
		targetValue := any(nil)

		mappedname, found := OtelToDatadogResource[name]
		if found {
			targetName = mappedname
			targetValue = v.AsString()
		} else {
			switch name {
			case "cardinalhq.fingerprint":
				targetName = "_fingerprint"
				targetValue = v.AsRaw()
			case "cardinalhq.filtered":
				targetName = "_filtered"
				targetValue = v.AsRaw()
			case "cardinalhq.rule_id":
				targetName = "_rule_id"
				targetValue = v.AsRaw()
			case "cardinalhq.cluster_id":
				targetName = "_cluster_id"
				targetValue = v.AsRaw()
			case "cardinalhq.provider":
				targetName = "_provider"
				targetValue = v.AsRaw()
			case "service.name":
				targetName = "service"
				targetValue = v.AsRaw()
			case "service.version":
				targetName = "version"
				targetValue = v.AsRaw()
			default:
				targetName = name
				targetValue = v.AsString()
			}
		}

		if _, found := m[targetName]; found {
			m[prefix+"."+targetName] = targetValue
		} else {
			m[targetName] = targetValue
		}

		return true
	})
}
