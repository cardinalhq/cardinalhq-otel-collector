// Copyright 2024 CardinalHQ, Inc
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
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *beagle) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	e.Lock()
	defer e.Unlock()

	now := time.Now()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		serviceName := getServiceName(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				isSlow := false
				if isslowValue, found := sr.Attributes().Get(translate.CardinalFieldSpanIsSlow); found {
					isSlow = isslowValue.Bool()
				}
				fingerprint := getFingerprint(sr.Attributes())
				if err := e.recordSpan(now, serviceName, fingerprint, isSlow, sr, iss, rs); err != nil {
					e.logger.Error("Failed to record span", zap.Error(err))
				}
			}
		}
	}

	return td, nil
}

func toAttributesAndSize(attributes map[string]interface{}) (map[string]string, int64) {
	result := make(map[string]string)
	var size int64 = 0
	for key, value := range attributes {
		size += int64(len(key) + len(fmt.Sprintf("%v", value)))
		result[key] = fmt.Sprintf("%v", value)
	}
	return result, size
}

func getOrElse(m map[string]string, key string, defaultValue string) string {
	if value, ok := m[key]; ok {
		return value
	}
	return defaultValue
}

func (e *beagle) recordSpan(now time.Time,
	serviceName string,
	fingerprint int64,
	isSlow bool,
	span ptrace.Span,
	iss ptrace.ScopeSpans,
	rs ptrace.ResourceSpans,
) error {
	// spanSize = (size of attributes + top level fields)
	var stringAttributes, spanSize = toAttributesAndSize(span.Attributes().AsRaw())
	spanSize += int64(len(span.TraceID().String()))
	spanSize += int64(len(span.Name()))
	spanSize += int64(len(span.Kind().String()))
	spanSize += int64(len(span.SpanID().String()))

	tagsToEnrich := map[string]string{
		"span.status_code": span.Status().Code().String(),
		"span.is_slow":     fmt.Sprintf("%v", isSlow),
		"span.kind":        span.Kind().String(),
	}

	otherTags := e.processEnrichments(e.config.Statistics.TracesEnrichments, map[string]pcommon.Map{
		"resource": rs.Resource().Attributes(),
		"scope":    iss.Scope().Attributes(),
		"span":     span.Attributes(),
	})

	// append enrichedTags to tagsToEnrich
	for key, value := range otherTags {
		tagsToEnrich[key] = value
	}

	rec := &chqpb.SpanStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		VendorId:    e.config.Statistics.Vendor,
		Count:       1,
		Tags:        tagsToEnrich,
		Exemplar: &chqpb.SpanExemplar{
			TraceId:      span.TraceID().String(),
			SpanName:     span.Name(),
			SpanKind:     span.Kind().String(),
			Resource:     getOrElse(stringAttributes, translate.CardinalFieldResourceName, ""),
			ResourceTags: ToMap(rs.Resource().Attributes()),
			ScopeTags:    ToMap(iss.Scope().Attributes()),
			SpanTags:     ToMap(span.Attributes()),
		},
	}
	bucketpile, err := e.spanStats.Record(now, rec, "", 1, spanSize)
	if err != nil {
		return err
	}
	if bucketpile != nil {
		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendSpanStats(context.Background(), now, bucketpile)
	}
	return nil
}

func (e *beagle) sendSpanStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.SpanStats) {
	wrapper := &chqpb.SpanStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.SpanStats{},
	}
	for _, items := range *bucketpile {
		wrapper.Stats = append(wrapper.Stats, items...)
	}

	if err := e.postSpanStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send span stats", zap.Error(err))
	}
	e.logger.Info("Sent log stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *beagle) postSpanStats(ctx context.Context, wrapper *chqpb.SpanStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	e.logger.Info("Sending span stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Statistics.Endpoint + "/api/v1/spanstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send span stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}