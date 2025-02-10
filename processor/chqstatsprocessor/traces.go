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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (p *statsProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
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
				isSlow := false
				if isslowValue, found := sr.Attributes().Get(translate.CardinalFieldSpanIsSlow); found {
					isSlow = isslowValue.Bool()
				}
				fingerprint := getFingerprint(sr.Attributes())
				if err := p.recordSpan(now, ee, serviceName, fingerprint, isSlow, sr, iss, rs); err != nil {
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
	isSlow bool,
	span ptrace.Span,
	iss ptrace.ScopeSpans,
	rs ptrace.ResourceSpans,
) error {
	// spanSize = (size of attributes + top level fields)
	var spanSize = toSize(span.Attributes().AsRaw())
	spanSize += int64(len(span.TraceID().String()))
	spanSize += int64(len(span.Name()))
	spanSize += int64(len(span.Kind().String()))
	spanSize += int64(len(span.SpanID().String()))

	enrichmentAttributes := p.processEnrichments(map[string]pcommon.Map{
		"resource": rs.Resource().Attributes(),
		"scope":    iss.Scope().Attributes(),
		"span":     span.Attributes(),
	})

	spanKindAttribute := toAttribute("span", "kind", pcommon.NewValueStr(span.Kind().String()), false)
	statusCodeAttribute := toAttribute("span", "status_code", pcommon.NewValueStr(span.Status().Code().String()), false)
	isSlowAttribute := toAttribute("span", "isSlow", pcommon.NewValueBool(isSlow), true)

	enrichmentAttributes = append(enrichmentAttributes, statusCodeAttribute)
	enrichmentAttributes = append(enrichmentAttributes, isSlowAttribute)
	enrichmentAttributes = append(enrichmentAttributes, spanKindAttribute)

	err := p.spanStats.Record(serviceName, fingerprint, p.pbPhase, p.id.Name(), environment.CollectorID(), environment.CustomerID(), enrichmentAttributes, 1, spanSize)
	if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
		telemetry.CounterAdd(p.cacheFull, 1)
	}
	p.addSpanExemplar(rs, iss, span, serviceName, fingerprint)

	telemetry.HistogramRecord(p.recordLatency, int64(time.Since(now)))

	return nil
}

func (p *statsProcessor) addSpanExemplar(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span, serviceName string, fingerprint int64) {
	if p.pbPhase == chqpb.Phase_PRE {
		key := p.toExemplarKey(serviceName, fingerprint)
		if p.logExemplars.Contains(key) {
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
		p.traceExemplars.Put(key, marshalled)
	}
}

func (p *statsProcessor) sendSpanStats(statsList []*chqpb.EventStats) {
	for _, stat := range statsList {
		key := p.toExemplarKey(stat.ServiceName, stat.Fingerprint)
		exemplarBytes, found := p.traceExemplars.Get(key)
		if !found {
			continue
		}
		stat.Exemplar = exemplarBytes.([]byte)
	}

	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Stats:       statsList}

	if err := p.postSpanStats(context.Background(), wrapper); err != nil {
		p.logger.Error("Failed to send span stats", zap.Error(err))
	}
}

func (p *statsProcessor) postSpanStats(ctx context.Context, wrapper *chqpb.EventStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(p.statsBatchSize, int64(len(b)))
	endpoint := p.config.Endpoint + "/api/v1/spanstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send span stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
