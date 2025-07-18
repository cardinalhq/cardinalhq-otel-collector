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
	"bytes"
	"context"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/oteltools/pkg/translate"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cardinalhq/oteltools/pkg/graph"
	"go.opentelemetry.io/collector/pdata/pcommon"
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

				// Add Span Kind to the attributes map so it can be used during relationship extraction
				spanAttributes := pcommon.NewMap()
				sr.Attributes().CopyTo(spanAttributes)
				spanAttributes.PutStr(graph.SpanKindString, sr.Kind().String())

				cache.ProvisionRecordAttributes(globalEntityMap, spanAttributes)
				fingerprint := fingerprinter.CalculateSpanFingerprint(rs.Resource(), sr)

				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				e.addSpanExemplar(cid, rs, iss, sr, fingerprint)
			}
		}
	}

	return nil
}

func (e *entityGraphExporter) sendExemplarPayload(cid string) func(payload []*SpanEntry) {
	return func(payload []*SpanEntry) {
		report := &chqpb.ExemplarPublishReport{
			OrganizationId: cid,
			ProcessorId:    e.id.Name(),
			TelemetryType:  e.ttype,
			Exemplars:      make([]*chqpb.Exemplar, 0),
		}
		for _, entry := range payload {
			me, err := e.jsonMarshaller.tracesMarshaler.MarshalTraces(entry.Exemplar)
			if err != nil {
				continue
			}
			exemplar := &chqpb.Exemplar{
				Attributes:  entry.toAttributes(),
				PartitionId: entry.Key,
				Payload:     string(me),
			}
			report.Exemplars = append(report.Exemplars, exemplar)
		}

		marshalled, err := proto.Marshal(report)
		if err != nil {
			e.logger.Error("Failed to marshal exemplars", zap.Error(err), zap.String("cid", cid))
			return
		}

		endpoint := fmt.Sprintf("%s/api/v1/exemplars/%s", e.config.Endpoint, e.ttype)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(marshalled))
		if err != nil {
			e.logger.Error("Failed to create request for exemplars", zap.Error(err), zap.String("cid", cid))
			return
		}

		req.Header.Set("Content-Type", "application/x-protobuf")

		resp, err := e.httpClient.Do(req)
		e.logger.Info("Sending trace exemplars", zap.String("cid", cid), zap.Int("count", len(payload)), zap.String("endpoint", endpoint),
			zap.Int("response_code", resp.StatusCode))

		if err != nil {
			e.logger.Error("Failed to send exemplars", zap.Error(err), zap.String("endpoint", endpoint))
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			e.logger.Error("Failed to send trace exemplars", zap.Int("status", resp.StatusCode), zap.String("body", string(body)), zap.String("endpoint", endpoint))
			return
		}
	}
}

func (e *entityGraphExporter) addSpanExemplar(cid string, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span, fingerprint int64) {
	rattr := rs.Resource().Attributes()
	spanKindInt := int32(sr.Kind())
	attributes := []string{
		translate.CardinalFieldFingerprint, strconv.FormatInt(fingerprint, 10),
		translate.CardinalFieldSpanKind, strconv.FormatInt(int64(spanKindInt), 10),
		translate.CardinalFieldSpanName, sr.Name(),
		string(semconv.ServiceNameKey), fingerprinter.GetFromResource(rattr, string(semconv.ServiceNameKey)),
		string(semconv.K8SClusterNameKey), fingerprinter.GetFromResource(rattr, string(semconv.K8SClusterNameKey)),
		string(semconv.K8SNamespaceNameKey), fingerprinter.GetFromResource(rattr, string(semconv.K8SNamespaceNameKey)),
	}
	spanId := sr.SpanID().String()
	parentSpanId := sr.ParentSpanID().String()

	cache, sok := e.spanExemplarCaches.Load(cid)
	if !sok {
		cache = NewSpanCache(5*time.Minute, 1*time.Minute, e.sendExemplarPayload(cid))
		e.spanExemplarCaches.Store(cid, cache)
	}
	contains := cache.Contains(spanId, fingerprint)
	if contains {
		return
	}
	exemplarRecord := toSpanExemplar(rs, ss, sr)
	cache.Put(spanId, parentSpanId, fingerprint, exemplarRecord, attributes)
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
