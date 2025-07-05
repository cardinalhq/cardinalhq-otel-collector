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

package chqexemplarprocessor

import (
	"context"
	"strconv"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (p *exemplarProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	//if !p.config.Reporting.Traces.Enabled {
	//	return td, nil
	//}
	//
	//for i := range td.ResourceSpans().Len() {
	//	rs := td.ResourceSpans().At(i)
	//	resourceAttributes := rs.Resource().Attributes()
	//	cid := orgIdFromResource(resourceAttributes)
	//	tenant := p.getTenant(cid)
	//	for j := range rs.ScopeSpans().Len() {
	//		iss := rs.ScopeSpans().At(j)
	//		for k := range iss.Spans().Len() {
	//			sr := iss.Spans().At(k)
	//			fingerprint := getFingerprint(sr.Attributes())
	//			p.addSpanExemplar(tenant, rs, iss, sr, fingerprint)
	//		}
	//	}
	//}

	return td, nil
}

func (p *exemplarProcessor) addSpanExemplar(tenant *Tenant, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span, fingerprint int64) {
	extraKeys := []string{
		translate.CardinalFieldFingerprint, strconv.FormatInt(fingerprint, 10),
	}
	keys, exemplarKey := computeExemplarKey(rs.Resource(), extraKeys)
	if tenant.traceCache.Contains(exemplarKey) {
		return
	}
	exemplarRecord := toSpanExemplar(rs, ss, sr)
	tenant.traceCache.Put(exemplarKey, keys, exemplarRecord)
}

func toSpanExemplar(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span) ptrace.Traces {
	exemplarRecord := ptrace.NewTraces()
	copyRl := exemplarRecord.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(copyRl.Resource())
	copySl := copyRl.ScopeSpans().AppendEmpty()
	ss.Scope().CopyTo(copySl.Scope())
	copyLr := copySl.Spans().AppendEmpty()
	sr.CopyTo(copyLr)
	return exemplarRecord
}
