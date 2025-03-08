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

package chqstatsprocessor

import (
	"context"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"strconv"
)

func (p *statsProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if !p.config.Statistics.Traces.StatisticsEnabled && !p.config.Statistics.Traces.ExemplarsEnabled {
		return td, nil
	}
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttributes := rs.Resource().Attributes()
		cid := OrgIdFromResource(resourceAttributes)
		tenant := p.getTenant(cid)

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				fingerprint := getFingerprint(sr.Attributes())
				if p.config.Statistics.Traces.ExemplarsEnabled {
					p.addSpanExemplar(tenant, rs, iss, sr, fingerprint)
				}
			}
		}
	}

	return td, nil
}

func (p *statsProcessor) addSpanExemplar(tenant *Tenant, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span, fingerprint int64) {
	if p.pbPhase != chqpb.Phase_PRE {
		return
	}

	keys, exemplarKey := computeExemplarKey(rs.Resource(), []string{translate.CardinalFieldFingerprint, strconv.FormatInt(fingerprint, 10)})
	if tenant.logExemplars.Contains(exemplarKey) {
		return
	}

	if tenant.traceExemplars.Contains(exemplarKey) {
		return
	}
	exemplarLd := ptrace.NewTraces()
	copyRl := exemplarLd.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(copyRl.Resource())
	copySl := copyRl.ScopeSpans().AppendEmpty()
	ss.Scope().CopyTo(copySl.Scope())
	copyLr := copySl.Spans().AppendEmpty()
	sr.CopyTo(copyLr)
	tenant.traceExemplars.Put(exemplarKey, keys, exemplarLd)
}
