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

package chqs3exporter

import (
	"encoding/json"
	"fmt"
)

func (e *s3Exporter) updateTagMap(customerID string, interval int64, tags map[string]any) error {
	e.taglock.Lock()
	defer e.taglock.Unlock()
	if _, ok := e.tags[customerID]; !ok {
		e.tags[customerID] = map[int64]map[string]any{}
	}
	if _, ok := e.tags[customerID][interval]; !ok {
		e.tags[customerID][interval] = map[string]any{}
	}
	for k, v := range tags {
		v = handleValue(v)
		current, ok := e.tags[customerID][interval][k]
		if ok {
			if fmt.Sprintf("%T", current) != fmt.Sprintf("%T", v) {
				return fmt.Errorf("Mismatched types: key = %s: %T %T", k, current, v)
			}
		} else {
			e.tags[customerID][interval][k] = v
		}
	}
	return nil
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
