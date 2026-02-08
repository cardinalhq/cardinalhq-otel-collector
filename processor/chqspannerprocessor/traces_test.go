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

package chqspannerprocessor

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqspannerprocessor/internal/metadata"
)

func TestConsumeTraces(t *testing.T) {
	unmarshaller := ptrace.JSONUnmarshaler{}

	settings := processortest.NewNopSettings(metadata.Type)
	p, err := newSpanner(&Config{}, settings)
	if err != nil {
		t.Fatalf("Failed to create chqspanner processor: %v", err)
	}

	testcase1, err := os.ReadFile("testdata/go-client-span.json")
	if err != nil {
		t.Fatalf("Failed to read test file: %v", err)
	}
	traces, err := unmarshaller.UnmarshalTraces(testcase1)
	if err != nil {
		t.Fatalf("Failed to unmarshal test traces: %v", err)
	}
	ctx := context.Background()
	result, err := p.ConsumeTraces(ctx, traces)
	require.NoError(t, err)

	require.Equal(t, 2, result.ResourceSpans().Len(), "Expected one ResourceSpans after ConsumeTraces")
	rs0 := result.ResourceSpans().At(0)
	ss0 := rs0.ScopeSpans()
	require.Equal(t, 1, ss0.Len(), "Expected one ScopeSpans after ConsumeTraces @ 0")
	spans0 := ss0.At(0).Spans()
	require.Equal(t, 1, spans0.Len(), "Expected two Spans after ConsumeTraces @ 0")
	span0 := spans0.At(0)

	rs1 := result.ResourceSpans().At(1)
	ss1 := rs1.ScopeSpans()
	require.Equal(t, 1, ss1.Len(), "Expected one ScopeSpans after ConsumeTraces @ 1")
	spans1 := ss1.At(0).Spans()
	require.Equal(t, 1, spans1.Len(), "Expected one Spans after ConsumeTraces @ 1")
	span1 := spans1.At(0)

	require.Equal(t, ptrace.SpanKindServer, span1.Kind(), "Expected span kind to be Server")

	serviceName, ok := rs1.Resource().Attributes().Get("service.name")
	require.True(t, ok, "Expected service.name attribute to be present")
	require.Equal(t, "prod-us-east-2-global", serviceName.AsString(), "Expected service.address to match")

	dbSystemName, ok := span1.Attributes().Get("db.system.name")
	require.True(t, ok, "Expected db.system.name attribute to be present")
	require.Equal(t, "postgresql", dbSystemName.AsString(), "Expected db.system.name to match")

	isSynthetic, ok := span1.Attributes().Get("synthetic")
	require.True(t, ok, "Expected synthetic attribute to be present")
	require.Equal(t, true, isSynthetic.AsRaw(), "Expected synthetic attribute to be true")

	require.Equal(t, span0.Name(), span1.Name(), "Expected span names to match")
}

func TestIsInterestingSpan(t *testing.T) {
	tests := []struct {
		name       string
		kind       ptrace.SpanKind
		attrs      map[string]any
		wantResult bool
	}{
		{
			name:       "Not client span",
			kind:       ptrace.SpanKindServer,
			attrs:      map[string]any{"db.system.name": "spanner", "service.name": "svc"},
			wantResult: false,
		},
		{
			name:       "Missing db.system.name",
			kind:       ptrace.SpanKindClient,
			attrs:      map[string]any{"service.name": "svc"},
			wantResult: false,
		},
		{
			name:       "serviceNameFromServerAddress returns empty",
			kind:       ptrace.SpanKindClient,
			attrs:      map[string]any{"db.system.name": "spanner"},
			wantResult: false,
		},
		{
			name:       "All conditions met",
			kind:       ptrace.SpanKindClient,
			attrs:      map[string]any{"db.system.name": "spanner", "server.address": "prod-us-east-2-global.cluster-zzz.us-east-2.rds.amazonaws.com"},
			wantResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span := ptrace.NewSpan()
			span.SetKind(tt.kind)
			for k, v := range tt.attrs {
				switch val := v.(type) {
				case string:
					span.Attributes().PutStr(k, val)
				case int:
					span.Attributes().PutInt(k, int64(val))
				case bool:
					span.Attributes().PutBool(k, val)
				}
			}
			got := isInterestingSpan(span)
			if got != tt.wantResult {
				t.Errorf("isInterestingSpan() = %v, want %v", got, tt.wantResult)
			}
		})
	}
}
