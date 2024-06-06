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

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (l *TableTranslator) TracesFromOtel(ot *ptrace.Traces) ([]map[string]any, error) {
	rets := []map[string]any{}

	for i := 0; i < ot.ResourceSpans().Len(); i++ {
		rs := ot.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				span := iss.Spans().At(k)
				ret := map[string]any{"_cardinalhq.telemetry_type": "traces"}
				addAttributes(ret, rs.Resource().Attributes(), "resource")
				addAttributes(ret, iss.Scope().Attributes(), "scope")
				addAttributes(ret, span.Attributes(), "span")
				ts := span.StartTimestamp().AsTime().UnixMilli()
				ret["_cardinalhq.timestamp"] = ts
				ret["_cardinalhq.span_eventcount"] = int32(span.Events().Len())
				ret["_cardinalhq.id"] = l.idg.Make(time.Now())
				ret["_cardinalhq.span_name"] = span.Name()
				ret["_cardinalhq.span_trace_id"] = span.TraceID().String()
				ret["_cardinalhq.span_span_id"] = span.SpanID().String()
				ret["_cardinalhq.span_parent_span_id"] = span.ParentSpanID().String()
				ret["_cardinalhq.span_kind"] = span.Kind().String()
				ret["_cardinalhq.span_status_code"] = span.Status().Code().String()
				ret["_cardinalhq.span_status_message"] = span.Status().Message()
				ret["_cardinalhq.span_duration"] = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Milliseconds()
				ret["_cardinalhq.span_start_time"] = span.StartTimestamp().AsTime().UnixMilli()
				ret["_cardinalhq.span_end_time"] = span.EndTimestamp().AsTime().UnixMilli()
				ret["_cardinalhq.scope_schemaurl"] = iss.SchemaUrl()
				ret["_cardinalhq.resource_schemaurl"] = rs.SchemaUrl()
				ensureExpectedKeysTraces(ret)
				rets = append(rets, ret)
			}
		}
	}

	return rets, nil
}

func ensureExpectedKeysTraces(m map[string]any) {
	keys := map[string]any{
		"_cardinalhq.hostname": findHostname(m),
		"_cardinalhq.value":    float64(1),
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
}
