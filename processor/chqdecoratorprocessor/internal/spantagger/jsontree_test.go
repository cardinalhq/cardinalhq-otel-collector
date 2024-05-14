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

package spantagger

import (
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

	ils := rs.ScopeSpans().AppendEmpty()

	// span1 is the root.
	span1 := ils.Spans().AppendEmpty()
	span1.SetTraceID(traceID)
	span1.SetSpanID(span1ID)
	span1.SetParentSpanID(pcommon.SpanID([8]byte{}))
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindClient)
	span1.Status().SetCode(ptrace.StatusCodeOk)

	// span2 is a child of span1.
	span2 := ils.Spans().AppendEmpty()
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
	span4 := ils.Spans().AppendEmpty()
	span4.SetTraceID(traceID)
	span4.SetSpanID(span4ID)
	span4.SetParentSpanID(span3ID)
	span4.SetName("span4")
	span4.SetKind(ptrace.SpanKindServer)
	span4.Status().SetCode(ptrace.StatusCodeOk)

	root, hasError, err := BuildTree(traces)
	assert.False(t, hasError)
	assert.NoError(t, err)
	assert.Equal(t, spanNode{
		TraceID:     traceID.String(),
		ServiceName: "example-service",
		SpanID:      span1ID.String(),
		SpanName:    "span1",
		SpanKind:    "Client",
		StatusCode:  "Ok",
		Children: []spanNode{
			{
				SpanID:     span2ID.String(),
				SpanName:   "span2",
				SpanKind:   "Server",
				StatusCode: "Ok",
			},
			{
				SpanID:     span3ID.String(),
				SpanName:   "span3",
				SpanKind:   "Server",
				StatusCode: "Ok",
				Children: []spanNode{
					{
						SpanID:     span4ID.String(),
						SpanName:   "span4",
						SpanKind:   "Server",
						StatusCode: "Ok",
					},
				},
			},
		},
	}, root)
}

func TestTreeToJSON(t *testing.T) {
	root := spanNode{
		TraceID:     "1234567890abcdef",
		ServiceName: "example-service",
		SpanID:      "abcdefgh",
		SpanName:    "span1",
		SpanKind:    "Client",
		StatusCode:  "Ok",
		Children: []spanNode{
			{
				SpanID:     "ijklmnop",
				SpanName:   "span2",
				SpanKind:   "Server",
				StatusCode: "Ok",
			},
			{
				SpanID:     "qrstuvwxyz",
				SpanName:   "span3",
				SpanKind:   "Server",
				StatusCode: "Ok",
				Children: []spanNode{
					{
						SpanID:     "01234567",
						SpanName:   "span4",
						SpanKind:   "Server",
						StatusCode: "Ok",
					},
				},
			},
		},
	}

	expectedJSON := `{"traceID":"1234567890abcdef","serviceName":"example-service","spanID":"abcdefgh","spanName":"span1","spanKind":"Client","statusCode":"Ok","children":[{"spanID":"ijklmnop","spanName":"span2","spanKind":"Server","statusCode":"Ok"},{"spanID":"qrstuvwxyz","spanName":"span3","spanKind":"Server","statusCode":"Ok","children":[{"spanID":"01234567","spanName":"span4","spanKind":"Server","statusCode":"Ok"}]}]}`

	jsonStr := TreeToJSON(root)
	assert.Equal(t, expectedJSON, jsonStr)
}