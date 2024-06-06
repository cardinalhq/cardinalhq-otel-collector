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
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

func (l *TableTranslator) LogsFromOtel(ol *plog.Logs) ([]map[string]any, error) {
	rets := []map[string]any{}

	for i := 0; i < ol.ResourceLogs().Len(); i++ {
		rl := ol.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			ill := rl.ScopeLogs().At(j)
			for k := 0; k < ill.LogRecords().Len(); k++ {
				log := ill.LogRecords().At(k)
				ret := map[string]any{"_cardinalhq.telemetry_type": "logs"}
				addAttributes(ret, rl.Resource().Attributes(), "resource")
				addAttributes(ret, ill.Scope().Attributes(), "scope")
				addAttributes(ret, log.Attributes(), "log")
				ret["_cardinalhq.message"] = log.Body().AsString()
				ts := log.Timestamp().AsTime().UnixMilli()
				if ts == 0 {
					ts = log.ObservedTimestamp().AsTime().UnixMilli()
				}
				ret["_cardinalhq.timestamp"] = ts
				ret["_cardinalhq.id"] = l.idg.Make(time.Now())
				ensureExpectedKeysLogs(ret)
				rets = append(rets, ret)
			}
		}
	}

	return rets, nil
}

func ensureExpectedKeysLogs(m map[string]any) {
	keys := map[string]any{
		"_cardinalhq.fingerprint": int64(0),
		"_cardinalhq.filtered":    false,
		"_cardinalhq.rule_id":     "",
		"_cardinalhq.ruleconfig":  "",
		"_cardinalhq.hostname":    findHostname(m),
		"_cardinalhq.message":     "",
		"_cardinalhq.value":       float64(1),
		"_cardinalhq.name":        "log_events",
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
}
