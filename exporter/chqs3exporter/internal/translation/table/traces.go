package table

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (l *TableTranslator) TracesFromOtel(ot *ptrace.Traces) ([]map[string]any, error) {
	rets := []map[string]any{}

	dumptrace(ot)

	for i := 0; i < ot.ResourceSpans().Len(); i++ {
		rs := ot.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				span := iss.Spans().At(k)
				ret := map[string]any{"_telemetry_type": "trace"}
				addAttributes(ret, rs.Resource().Attributes(), "resource")
				addAttributes(ret, iss.Scope().Attributes(), "scope")
				addAttributes(ret, span.Attributes(), "span")
				ts := span.StartTimestamp().AsTime().UnixMilli()
				ret["timestamp"] = ts
				ret["_span_eventcount"] = int32(span.Events().Len())
				ret["_id"] = l.idg.Make(time.Now())
				ret["_span_name"] = span.Name()
				ret["_span_trace_id"] = span.TraceID().String()
				ret["_span_span_id"] = span.SpanID().String()
				ret["_span_parent_span_id"] = span.ParentSpanID().String()
				ret["_span_kind"] = span.Kind().String()
				ret["_span_status_code"] = span.Status().Code().String()
				ret["_span_status_message"] = span.Status().Message()
				ret["_span_duration"] = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Milliseconds()
				ret["_span_start_time"] = span.StartTimestamp().AsTime().UnixMilli()
				ret["_span_end_time"] = span.EndTimestamp().AsTime().UnixMilli()
				ret["_scope_schemaurl"] = iss.SchemaUrl()
				ret["_resource_schemaurl"] = rs.SchemaUrl()
				ensureExpectedKeysTraces(ret)
				rets = append(rets, ret)
			}
		}
	}

	return rets, nil
}

var fn = 0

func dumptrace(ot *ptrace.Traces) {
	jt := ptrace.JSONMarshaler{}
	b, _ := jt.MarshalTraces(*ot)

	filename := fmt.Sprintf("trace-%08d.json", fn)
	fn++
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	// reformat
	var v map[string]any
	err = json.Unmarshal(b, &v)
	if err != nil {
		fmt.Println(err)
		return
	}

	b, err = json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Println(err)
		return
	}
	f.Write(b)
}

func ensureExpectedKeysTraces(m map[string]any) {
	keys := map[string]any{
		"_cluster_id": "",
		"_provider":   "",
		"service":     "unknown_service",
		"version":     "",
		"hostname":    findHostname(m),
		"value":       float64(1),
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
}
