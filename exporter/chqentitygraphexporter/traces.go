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

package chqentitygraphexporter

import (
	"context"

	"github.com/cardinalhq/oteltools/pkg/graph"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (e *entityGraphExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	for i := range td.ResourceSpans().Len() {
		rs := td.ResourceSpans().At(i)
		resourceAttributes := rs.Resource().Attributes()
		cid := orgIdFromResource(resourceAttributes)
		cache := e.getEntityCache(cid)
		globalEntityMap := cache.ProvisionResourceAttributes(resourceAttributes)

		for j := range rs.ScopeSpans().Len() {
			iss := rs.ScopeSpans().At(j)
			for k := range iss.Spans().Len() {
				sr := iss.Spans().At(k)

				// Add span kind to the attributes map so it can be used during relationship extraction
				spanKind := sr.Kind().String()
				sr.Attributes().PutStr(graph.SpanKindString, spanKind)

				cache.ProvisionRecordAttributes(globalEntityMap, sr.Attributes())
			}
		}
	}

	return nil
}
