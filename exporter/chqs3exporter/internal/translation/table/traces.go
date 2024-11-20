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

	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (l *TableTranslator) TracesFromOtel(ot *ptrace.Traces, environment translate.Environment) ([]map[string]any, error) {
	rets := []map[string]any{}

	for i := 0; i < ot.ResourceSpans().Len(); i++ {
		rs := ot.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				span := iss.Spans().At(k)
				ret := map[string]any{translate.CardinalFieldTelemetryType: translate.CardinalTelemetryTypeTraces}
				addAttributes(ret, rs.Resource().Attributes(), "resource")
				addAttributes(ret, iss.Scope().Attributes(), "scope")
				addAttributes(ret, span.Attributes(), "span")
				ts := span.StartTimestamp().AsTime().UnixMilli()
				ret[translate.CardinalFieldTimestamp] = ts
				ret[translate.CardinalFieldSpanEventcount] = int32(span.Events().Len())
				ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
				ret[translate.CardinalFieldSpanName] = span.Name()
				ret[translate.CardinalFieldSpanTraceID] = span.TraceID().String()
				ret[translate.CardinalFieldSpanSpanID] = span.SpanID().String()
				ret[translate.CardinalFieldSpanParentSpanID] = span.ParentSpanID().String()
				ret[translate.CardinalFieldSpanKind] = span.Kind().String()
				ret[translate.CardinalFieldSpanStatusCode] = span.Status().Code().String()
				ret[translate.CardinalFieldSpanStatusMessage] = span.Status().Message()
				ret[translate.CardinalFieldSpanDuration] = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Milliseconds()
				ret[translate.CardinalFieldSpanStartTime] = span.StartTimestamp().AsTime().UnixMilli()
				ret[translate.CardinalFieldSpanEndTime] = span.EndTimestamp().AsTime().UnixMilli()
				ret[translate.CardinalFieldScopeSchemaURL] = iss.SchemaUrl()
				ret[translate.CardinalFieldResourceSchemaURL] = rs.SchemaUrl()
				if environment != nil {
					for k, v := range environment.Tags() {
						ret["env."+sanitizeAttribute(k)] = v
					}
				}
				ensureExpectedKeysTraces(ret)
				rets = append(rets, ret)
			}
		}
	}

	return rets, nil
}

func ensureExpectedKeysTraces(m map[string]any) {
	keys := map[string]any{
		translate.CardinalFieldHostname: findHostname(m),
		translate.CardinalFieldValue:    float64(1),
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
}
