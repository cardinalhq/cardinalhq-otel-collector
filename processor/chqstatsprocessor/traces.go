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
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
)

func (p *statsProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if !p.config.Statistics.Traces.StatisticsEnabled && !p.config.Statistics.Traces.ExemplarsEnabled {
		return td, nil
	}

	ee := authenv.GetEnvironment(ctx, p.idSource)

	now := time.Now()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttributes := rs.Resource().Attributes()
		serviceName := getServiceName(resourceAttributes)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)

				fingerprint := getFingerprint(sr.Attributes())
				if err := p.recordSpan(now, ee, serviceName, fingerprint, sr, iss, rs); err != nil {
					p.logger.Error("Failed to record span", zap.Error(err))
				}
			}
		}
	}

	return td, nil
}

func toSize(attributes map[string]interface{}) int64 {
	var size int64 = 0
	for key, value := range attributes {
		size += int64(len(key) + len(fmt.Sprintf("%v", value)))
	}
	return size
}

func (p *statsProcessor) recordSpan(
	now time.Time,
	environment authenv.Environment,
	serviceName string,
	fingerprint int64,
	span ptrace.Span,
	iss ptrace.ScopeSpans,
	rs ptrace.ResourceSpans,
) error {
	orgID := OrgIdFromResource(rs.Resource().Attributes())
	tenant := p.getTenant(orgID) // TODO move this to the top of the resource loop

	if p.config.Statistics.Traces.StatisticsEnabled {
		// spanSize = (size of attributes + top level fields)
		var spanSize = toSize(span.Attributes().AsRaw())
		spanSize += int64(len(span.TraceID().String()))
		spanSize += int64(len(span.Name()))
		spanSize += int64(len(span.Kind().String()))
		spanSize += int64(len(span.SpanID().String()))

		enrichmentAttributes := p.processEnrichments(orgID,
			map[string]pcommon.Map{
				"resource": rs.Resource().Attributes(),
				"scope":    iss.Scope().Attributes(),
				"span":     span.Attributes(),
			})

		spanKindAttribute := toAttribute("span", "kind", pcommon.NewValueStr(span.Kind().String()), false)
		statusCodeAttribute := toAttribute("span", "status_code", pcommon.NewValueStr(span.Status().Code().String()), false)
		err := tenant.spanSketchCache.UpdateSpanSketch(serviceName, span)
		if err != nil {
			p.logger.Error("Failed to update span sketch cache", zap.Error(err))
		}
		enrichmentAttributes = append(enrichmentAttributes, statusCodeAttribute)
		enrichmentAttributes = append(enrichmentAttributes, spanKindAttribute)

		err = tenant.spanStats.Record(serviceName, fingerprint, p.pbPhase, p.id.Name(), environment.CollectorID(), environment.CustomerID(), enrichmentAttributes, 1, spanSize)
		if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
			telemetry.CounterAdd(p.cacheFull, 1)
		}
	}
	if p.config.Statistics.Traces.ExemplarsEnabled {
		p.addSpanExemplar(tenant, rs, iss, span, serviceName, fingerprint)
	}
	telemetry.HistogramRecord(p.recordLatency, int64(time.Since(now)))

	return nil
}

func (p *statsProcessor) addSpanExemplar(tenant *Tenant, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span, serviceName string, fingerprint int64) {
	if p.pbPhase != chqpb.Phase_PRE {
		return
	}

	key := p.toExemplarKey(serviceName, fingerprint)
	if tenant.traceExemplars.Contains(key) {
		return
	}
	exemplarLd := ptrace.NewTraces()
	copyRl := exemplarLd.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(copyRl.Resource())
	copySl := copyRl.ScopeSpans().AppendEmpty()
	ss.Scope().CopyTo(copySl.Scope())
	copyLr := copySl.Spans().AppendEmpty()
	sr.CopyTo(copyLr)
	marshalled, me := p.jsonMarshaller.tracesMarshaler.MarshalTraces(exemplarLd)
	if me != nil {
		return
	}
	tenant.traceExemplars.Put(key, marshalled)
}
