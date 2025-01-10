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

package spantagger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestBuildTree(t *testing.T) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "example-service")

	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span1ID := pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span2ID := pcommon.SpanID([8]byte{2, 3, 4, 5, 6, 7, 8, 9})
	span3ID := pcommon.SpanID([8]byte{3, 4, 5, 6, 7, 8, 9, 10})
	span4ID := pcommon.SpanID([8]byte{4, 5, 6, 7, 8, 9, 10, 11})
	span5ID := pcommon.SpanID([8]byte{5, 6, 7, 8, 9, 10, 11, 12})

	ils := rs.ScopeSpans().AppendEmpty()

	// span1 is the root.
	span1 := ils.Spans().AppendEmpty()
	span1.SetTraceID(traceID)
	span1.SetSpanID(span1ID)
	span1.SetParentSpanID(pcommon.SpanID([8]byte{}))
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindClient)
	span1.Status().SetCode(ptrace.StatusCodeOk)

	rs2 := traces.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "example-service2")
	ils2 := rs2.ScopeSpans().AppendEmpty()
	// span2 is a child of span1.
	span2 := ils2.Spans().AppendEmpty()
	span2.SetTraceID(traceID)
	span2.SetSpanID(span2ID)
	span2.SetParentSpanID(span1ID)
	span2.SetName("span2")
	span2.SetKind(ptrace.SpanKindServer)
	span2.Status().SetCode(ptrace.StatusCodeOk)

	// span3 is a child of span1.
	span3 := ils.Spans().AppendEmpty()
	span3.SetTraceID(traceID)
	span3.SetSpanID(span3ID)
	span3.SetParentSpanID(span1ID)
	span3.SetName("span3")
	span3.SetKind(ptrace.SpanKindServer)
	span3.Status().SetCode(ptrace.StatusCodeOk)

	// span4 is a child of span3.
	span4 := ils2.Spans().AppendEmpty()
	span4.SetTraceID(traceID)
	span4.SetSpanID(span4ID)
	span4.SetParentSpanID(span3ID)
	span4.SetName("span4")
	span4.SetKind(ptrace.SpanKindServer)
	span4.Status().SetCode(ptrace.StatusCodeOk)

	// span5 is a child of span3 with the same attributes as span4.
	span5 := ils2.Spans().AppendEmpty()
	span5.SetTraceID(traceID)
	span5.SetSpanID(span5ID)
	span5.SetParentSpanID(span3ID)
	span5.SetName("span4")
	span5.SetKind(ptrace.SpanKindServer)
	span5.Status().SetCode(ptrace.StatusCodeUnset)

	root, hasError, err := BuildTree(traces, 9999)
	assert.False(t, hasError)
	assert.NoError(t, err)

	exp := &Graph{
		Fingerprint: 9999,
		Graph: &spanNode{
			ServiceName: "example-service",
			SpanName:    "span1",
			SpanKind:    "Client",
			Children: []*spanNode{
				{
					ServiceName: "example-service",
					SpanName:    "span3",
					SpanKind:    "Server",
					Children: []*spanNode{
						{
							ServiceName: "example-service2",
							SpanName:    "span4",
							SpanKind:    "Server",
						},
					},
				},
				{
					ServiceName: "example-service2",
					SpanName:    "span2",
					SpanKind:    "Server",
				},
			},
		},
	}

	assert.Equal(t, exp, root)
}

func TestTreeToJSON(t *testing.T) {
	graph := &Graph{
		Fingerprint: 9999,
		Graph: &spanNode{
			ServiceName: "example-service",
			SpanName:    "span1",
			SpanKind:    "Client",
			Children: []*spanNode{
				{
					ServiceName: "example-service2",
					SpanName:    "span2",
					SpanKind:    "Server",
				},
				{
					ServiceName: "example-service3",
					SpanName:    "span3",
					SpanKind:    "Server",
					Children: []*spanNode{
						{
							ServiceName: "example-service4",
							SpanName:    "span4",
							SpanKind:    "Server",
						},
					},
				},
			},
		},
	}

	expectedJSON := `
{
	"fingerprint": 9999,
	"graph": {
		"serviceName": "example-service",
		"spanName": "span1",
		"spanKind": "Client",
		"children": [
			{
				"serviceName": "example-service2",
				"spanName": "span2",
				"spanKind": "Server"
			}, {
				"serviceName": "example-service3",
				"spanName": "span3",
				"spanKind": "Server",
				"children": [
					{
						"serviceName": "example-service4",
						"spanName": "span4",
						"spanKind": "Server"
					}
				]
			}
		]
	}
}`

	b, err := json.Marshal(graph)
	assert.NoError(t, err)
	assert.JSONEq(t, expectedJSON, string(b))
}
