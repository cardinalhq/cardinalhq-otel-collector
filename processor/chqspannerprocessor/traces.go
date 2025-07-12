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
	"net/url"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (p *chqspanner) ConsumeTraces(ctx context.Context, traces ptrace.Traces) (ptrace.Traces, error) {
	var toClone []ptrace.Span

	for i := range traces.ResourceSpans().Len() {
		resourceSpan := traces.ResourceSpans().At(i)
		for j := range resourceSpan.ScopeSpans().Len() {
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			for k := range scopeSpan.Spans().Len() {
				span := scopeSpan.Spans().At(k)
				if _, found := span.Attributes().Get("server.address"); !found {
					if dbc, found := span.Attributes().Get("db.connection_string"); found {
						parsed, err := url.Parse(dbc.AsString())
						if err != nil {
							continue
						}
						if parsed.Host == "" || parsed.Host == "localhost" || parsed.Host == "127.0.0.1" {
							continue
						}
						span.Attributes().PutStr("server.address", parsed.Host)
					}
				}
				if !isInterestingSpan(span) {
					continue
				}
				toClone = append(toClone, span)
			}
		}
	}
	if len(toClone) == 0 {
		return traces, nil
	}

	for _, span := range toClone {
		newResourceSpan := traces.ResourceSpans().AppendEmpty()
		newScopeSpan := newResourceSpan.ScopeSpans().AppendEmpty()
		newSpan := newScopeSpan.Spans().AppendEmpty()
		cloneFrom(newResourceSpan, newScopeSpan, span, newSpan)
	}
	return traces, nil
}

func isInterestingSpan(span ptrace.Span) bool {
	if span.Kind() != ptrace.SpanKindClient {
		return false
	}
	if _, found := span.Attributes().Get("db.system.name"); !found {
		return false
	}
	return serviceNameFromServerAddress(span) != ""
}

var copyAttrs = []string{
	"db.collection.name",
	"db.namespace",
	"db.system.name",
}

func cloneFrom(rs ptrace.ResourceSpans, _ ptrace.ScopeSpans, src ptrace.Span, dst ptrace.Span) {
	rs.Resource().Attributes().PutStr("service.name", serviceNameFromServerAddress(src))
	rs.Resource().Attributes().PutStr("k8s.cluster.name", "AWS")
	rs.Resource().Attributes().PutStr("k8s.namespace.name", "RDS")
	rs.Resource().Attributes().PutStr("cloud.provider", "aws")

	for _, attr := range copyAttrs {
		if v, found := src.Attributes().Get(attr); found {
			dst.Attributes().PutStr(attr, v.AsString())
		}
	}
	dst.SetName("ExecDBQuery")
	dst.SetKind(ptrace.SpanKindServer)
	dst.Attributes().PutBool("synthetic", true)
	dst.SetParentSpanID(src.SpanID())
	dst.SetTraceID(src.TraceID())
	dst.SetStartTimestamp(src.StartTimestamp())
	dst.SetEndTimestamp(src.EndTimestamp())
}
