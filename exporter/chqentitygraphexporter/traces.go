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
	"github.com/cardinalhq/oteltools/hashutils"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
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

const (
	serviceNameKey   = string(semconv.ServiceNameKey)
	clusterNameKey   = string(semconv.K8SClusterNameKey)
	namespaceNameKey = string(semconv.K8SNamespaceNameKey)
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
				e.addSpanExemplar(cid, rs, iss, sr, getFingerprint(spanAttributes))
			}
		}
	}

	return nil
}

func (e *entityGraphExporter) sendExemplarPayload(cid string) func(payload []*SpanEntry) {
	return func(payload []*SpanEntry) {
		e.logger.Info("Sending trace exemplars", zap.String("cid", cid), zap.Int("count", len(payload)))
		report := &chqpb.ExemplarPublishReport{
			OrganizationId: cid,
			ProcessorId:    e.id.String(),
			TelemetryType:  e.ttype,
			Exemplars:      make([]*chqpb.Exemplar, 0),
		}
		for _, entry := range payload {
			me, err := e.jsonMarshaller.tracesMarshaler.MarshalTraces(entry.exemplar)
			if err != nil {
				continue
			}
			exemplar := &chqpb.Exemplar{
				Attributes:  entry.toAttributes(),
				PartitionId: entry.key,
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
		return
	}
}

func (e *entityGraphExporter) addSpanExemplar(cid string, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sr ptrace.Span, fingerprint int64) {
	extraKeys := []string{
		translate.CardinalFieldFingerprint, strconv.FormatInt(fingerprint, 10),
	}
	keys, exemplarKey := computeExemplarKey(rs.Resource(), extraKeys)
	cache, sok := e.spanExemplarCaches.Load(cid)
	if !sok {
		cache = NewShardedSpanLRUCache(5*time.Minute, 15*time.Minute, e.sendExemplarPayload(cid))
		e.spanExemplarCaches.Store(cid, cache)
	}
	contains, shardIndex := cache.Contains(sr.TraceID(), exemplarKey)
	if contains {
		return
	}
	exemplarRecord := toSpanExemplar(rs, ss, sr)
	cache.PutWithShardIndex(shardIndex, exemplarKey, keys, fingerprint, exemplarRecord)
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

func computeExemplarKey(rl pcommon.Resource, extraKeys []string) ([]string, int64) {
	keys := []string{
		clusterNameKey, getFromResource(rl.Attributes(), clusterNameKey),
		namespaceNameKey, getFromResource(rl.Attributes(), namespaceNameKey),
		serviceNameKey, getFromResource(rl.Attributes(), serviceNameKey),
	}
	keys = append(keys, extraKeys...)
	return keys, int64(hashutils.HashStrings(nil, keys...))
}

func getFromResource(attr pcommon.Map, key string) string {
	clusterVal, clusterFound := attr.Get(key)
	if !clusterFound {
		return "unknown"
	}
	return clusterVal.AsString()
}

func getFingerprint(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}
