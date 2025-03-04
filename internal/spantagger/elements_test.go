// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

func TestMakeSpanElement(t *testing.T) {
	serviceName := "example-service"
	span := ptrace.NewSpan()
	span.SetName("example-span")
	span.SetKind(ptrace.SpanKindClient)
	span.Status().SetCode(ptrace.StatusCodeOk)
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span.SetParentSpanID(pcommon.SpanID([8]byte{8, 7, 6, 5, 4, 3, 2, 1}))

	expected := spanelement{
		TraceID:      "0102030405060708090a0b0c0d0e0f10",
		SpanID:       "0102030405060708",
		ParentSpanID: "0807060504030201",
		ServiceName:  "example-service",
		SpanName:     "example-span",
		SpanKind:     "Client",
		StatusCode:   "Ok",
	}

	result := makeSpanElement(serviceName, span)
	assert.Equal(t, expected, result)
}

func TestFindPathsDFS(t *testing.T) {
	elements := []spanelement{
		{SpanID: "1", ParentSpanID: "", ServiceName: "service1", SpanName: "span1", SpanKind: "Client"},
		{SpanID: "2", ParentSpanID: "1", ServiceName: "service1", SpanName: "span2", SpanKind: "Server"},
		{SpanID: "3", ParentSpanID: "2", ServiceName: "service1", SpanName: "span3", SpanKind: "Producer"},
		{SpanID: "4", ParentSpanID: "2", ServiceName: "service1", SpanName: "span4", SpanKind: "Consumer"},
		{SpanID: "5", ParentSpanID: "3", ServiceName: "service1", SpanName: "span5", SpanKind: "Client"},
	}

	expected := [][]spanelement{
		{
			{SpanID: "1", ParentSpanID: "", ServiceName: "service1", SpanName: "span1", SpanKind: "Client"},
			{SpanID: "2", ParentSpanID: "1", ServiceName: "service1", SpanName: "span2", SpanKind: "Server"},
			{SpanID: "3", ParentSpanID: "2", ServiceName: "service1", SpanName: "span3", SpanKind: "Producer"},
			{SpanID: "5", ParentSpanID: "3", ServiceName: "service1", SpanName: "span5", SpanKind: "Client"},
		},
		{
			{SpanID: "1", ParentSpanID: "", ServiceName: "service1", SpanName: "span1", SpanKind: "Client"},
			{SpanID: "2", ParentSpanID: "1", ServiceName: "service1", SpanName: "span2", SpanKind: "Server"},
			{SpanID: "4", ParentSpanID: "2", ServiceName: "service1", SpanName: "span4", SpanKind: "Consumer"},
		},
	}

	result := findPathsDFS(elements)
	assert.Equal(t, expected, result)
}

func TestJunkTrace(t *testing.T) {
	type test struct {
		name     string
		elements []spanelement
		expected error
	}

	tests := []test{
		{
			"valid trace",
			[]spanelement{
				{SpanID: "1", ParentSpanID: "", TraceID: "1"},
				{SpanID: "2", ParentSpanID: "1", TraceID: "1"},
			},
			nil,
		},
		{
			"more than one root",
			[]spanelement{
				{SpanID: "1", ParentSpanID: "", TraceID: "1"},
				{SpanID: "2", ParentSpanID: "", TraceID: "1"},
			},
			MultipleRootsError,
		},
		{
			"no root",
			[]spanelement{
				{SpanID: "1", ParentSpanID: "2", TraceID: "1"},
				{SpanID: "2", ParentSpanID: "3", TraceID: "1"},
			},
			NoRootError,
		},
		{
			"inconsistent trace IDs",
			[]spanelement{
				{SpanID: "1", ParentSpanID: "", TraceID: "1"},
				{SpanID: "2", ParentSpanID: "1", TraceID: "1"},
				{SpanID: "3", ParentSpanID: "2", TraceID: "2"},
			},
			InconsistentTraceIDsError,
		},
		{
			"orphaned span",
			[]spanelement{
				{SpanID: "1", ParentSpanID: "", TraceID: "1"},
				{SpanID: "2", ParentSpanID: "1", TraceID: "1"},
				{SpanID: "3", ParentSpanID: "4", TraceID: "1"},
			},
			OrphanedSpanError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := junkTrace(tt.elements)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSpanElementsWithServiceName(t *testing.T) {
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

	expected := []spanelement{
		{
			TraceID:      "0102030405060708090a0b0c0d0e0f10",
			SpanID:       "0102030405060708",
			ParentSpanID: "",
			ServiceName:  "example-service",
			SpanName:     "span1",
			SpanKind:     "Client",
			StatusCode:   "Ok",
		},
		{
			TraceID:      "0102030405060708090a0b0c0d0e0f10",
			SpanID:       "0203040506070809",
			ParentSpanID: "0102030405060708",
			ServiceName:  "example-service",
			SpanName:     "span2",
			SpanKind:     "Server",
			StatusCode:   "Ok",
		},
	}

	result := spanElements(traces)
	assert.Equal(t, expected, result)
}

func TestSpanElementsWithoutServiceName(t *testing.T) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()

	ils := rs.ScopeSpans().AppendEmpty()
	span1 := ils.Spans().AppendEmpty()
	span1.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span1.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span1.SetParentSpanID(pcommon.SpanID([8]byte{}))
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindClient)
	span1.Status().SetCode(ptrace.StatusCodeOk)

	expected := []spanelement{
		{
			TraceID:      "0102030405060708090a0b0c0d0e0f10",
			SpanID:       "0102030405060708",
			ParentSpanID: "",
			ServiceName:  "unknown",
			SpanName:     "span1",
			SpanKind:     "Client",
			StatusCode:   "Ok",
		},
	}

	result := spanElements(traces)
	assert.Equal(t, expected, result)
}

func TestAnyErrors(t *testing.T) {
	type test struct {
		name     string
		elements []spanelement
		expected bool
	}

	tests := []test{
		{
			"OKs",
			[]spanelement{
				{StatusCode: "Ok"},
			},
			false,
		},
		{
			"OK, Unset",
			[]spanelement{
				{StatusCode: "Ok"},
				{StatusCode: "Unset"},
			},
			false,
		},
		{
			"OK, Error",
			[]spanelement{
				{StatusCode: "Ok"},
				{StatusCode: "Error"},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := anyErrors(tt.elements)
			assert.Equal(t, tt.expected, result)
		})
	}
}
