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
	"github.com/cardinalhq/oteltools/pkg/translate"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

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
				computedFingerprint := fingerprinter.CalculateSpanFingerprint(rs.Resource(), sr)
				spanAttributes.PutInt(translate.CardinalFieldFingerprint, computedFingerprint)
				e.addSpanExemplar(cid, rs, iss, sr, computedFingerprint)
			}
		}
	}

	return nil
}

func (e *entityGraphExporter) sendExemplarPayload(cid string) func(spans []spanWrapper) {
	return func(spans []spanWrapper) {
		report := &chqpb.ExemplarPublishReport{
			OrganizationId: cid,
			ProcessorId:    e.id.Name(),
			TelemetryType:  e.ttype,
			Exemplars:      make([]*chqpb.Exemplar, 0),
		}
		for _, span := range spans {
			payload, err := e.jsonMarshaller.tracesMarshaler.MarshalTraces(span.exemplar)
			if err != nil {
				continue
			}

			if len(span.parentFingerprints) > 0 {
				var pfpList []string
				for pfp, _ := range span.parentFingerprints {
					pfpList = append(pfpList, strconv.FormatInt(pfp, 10))
				}
				span.attributes["parent.fingerprints"] = strings.Join(pfpList, ",")
			}
			if len(span.flowIds) > 0 {
				var flowIdList []string
				for flowId, _ := range span.flowIds {
					flowIdList = append(flowIdList, flowId)
				}
				span.attributes["flow.ids"] = strings.Join(flowIdList, ",")
			}
			exemplar := &chqpb.Exemplar{
				Attributes:  span.attributes,
				PartitionId: span.fingerprint,
				Payload:     string(payload),
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
		e.logger.Info("Sending trace exemplars", zap.String("cid", cid), zap.Int("count", len(spans)), zap.String("endpoint", endpoint),
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
	cache, sok := e.spanExemplarCaches.Load(cid)
	if !sok {
		cache = NewTraceCache(5*time.Minute, 1000, e.sendExemplarPayload(cid))
		e.spanExemplarCaches.Store(cid, cache)
	}

	attributes := make(map[string]string)
	attributes[string(semconv.ServiceNameKey)] = fingerprinter.GetFromResource(rs.Resource().Attributes(), string(semconv.ServiceNameKey))
	attributes[string(semconv.K8SClusterNameKey)] = fingerprinter.GetFromResource(rs.Resource().Attributes(), string(semconv.K8SClusterNameKey))
	attributes[string(semconv.K8SNamespaceNameKey)] = fingerprinter.GetFromResource(rs.Resource().Attributes(), string(semconv.K8SNamespaceNameKey))

	exemplarRecord := toSpanExemplar(rs, ss, sr)
	cache.Put(sr, exemplarRecord, fingerprint, attributes)
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
