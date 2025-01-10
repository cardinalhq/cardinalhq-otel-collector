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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestUniquePathStrings(t *testing.T) {
	pathstrings := []string{"D", "B", "B", "C", "C", "C", "D", "D", "D", "A"}
	expected := []string{"A", "B", "C", "D"}

	result := uniquePathStrings(pathstrings)
	assert.Equal(t, expected, result)
}

func TestElementPathString(t *testing.T) {
	element := spanelement{
		ServiceName: "example-service",
		SpanName:    "example-span",
		SpanKind:    "Client",
	}
	expected := "example-service:example-span:Client"

	result := elementPathString(element)
	assert.Equal(t, expected, result)
}

func TestSpanPathStrings(t *testing.T) {
	elementPaths := [][]spanelement{
		{
			{ServiceName: "service1", SpanName: "span1", SpanKind: "Client"},
			{ServiceName: "service1", SpanName: "span2", SpanKind: "Server"},
		},
		{
			{ServiceName: "service2", SpanName: "span3", SpanKind: "Producer"},
			{ServiceName: "service2", SpanName: "span4", SpanKind: "Consumer"},
			{ServiceName: "service2", SpanName: "span5", SpanKind: "Client"},
		},
	}

	expected := []string{
		"service1:span1:Client::service1:span2:Server",
		"service2:span3:Producer::service2:span4:Consumer::service2:span5:Client",
	}

	result := spanPathStrings(elementPaths)
	assert.Equal(t, expected, result)
}

func TestFingerprintConsistent(t *testing.T) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "example-service")

	ils := rs.ScopeSpans().AppendEmpty()
	span1 := ils.Spans().AppendEmpty()
	span1.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span1.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span1.SetParentSpanID(pcommon.SpanID([8]byte{}))
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindClient)
	span1.Status().SetCode(ptrace.StatusCodeOk)

	span2 := ils.Spans().AppendEmpty()
	span2.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span2.SetSpanID(pcommon.SpanID([8]byte{2, 3, 4, 5, 6, 7, 8, 9}))
	span2.SetParentSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span2.SetName("span2")
	span2.SetKind(ptrace.SpanKindServer)
	span2.Status().SetCode(ptrace.StatusCodeOk)

	result, hasErrors, err := Fingerprint(traces)
	assert.NoError(t, err)
	assert.False(t, hasErrors)
	assert.Equal(t, uint64(0x65631a3a1b9273e8), result)
}

func TestFingerprintInconsistent(t *testing.T) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "example-service")

	ils := rs.ScopeSpans().AppendEmpty()
	span1 := ils.Spans().AppendEmpty()
	span1.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span1.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span1.SetParentSpanID(pcommon.SpanID([8]byte{2, 3, 4, 5, 6, 7, 8, 9}))
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindClient)
	span1.Status().SetCode(ptrace.StatusCodeOk)

	_, _, err := Fingerprint(traces)
	assert.ErrorIs(t, NoRootError, err)
}
