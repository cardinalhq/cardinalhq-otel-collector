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

import (
	"encoding/json"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/cardinalhq/oteltools/pkg/translate"
)

func addAttributes(m map[string]any, attrs pcommon.Map, prefix string) {
	attrs.Range(func(name string, v pcommon.Value) bool {
		if strings.HasPrefix(name, translate.CardinalFieldPrefixDot) {
			m[name] = handleValue(v.AsRaw())
		} else {
			m[prefix+"."+sanitizeAttribute(name)] = v.AsString()
		}

		return true
	})
}

func handleValue(v any) any {
	switch v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return v
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return "[]"
		}
		return string(bytes)
	}
}
