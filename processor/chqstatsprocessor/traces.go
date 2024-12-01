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

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
)

func (e *statsProc) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	//now := time.Now()
	//for i := 0; i < td.ResourceSpans().Len(); i++ {
	//	rs := td.ResourceSpans().At(i)
	//	serviceName := getServiceName(rs.Resource().Attributes())
	//	for j := 0; j < rs.ScopeSpans().Len(); j++ {
	//		iss := rs.ScopeSpans().At(j)
	//		for k := 0; k < iss.Spans().Len(); k++ {
	//			sr := iss.Spans().At(k)
	//			isSlow := false
	//			if isslowValue, found := sr.Attributes().Get(translate.CardinalFieldSpanIsSlow); found {
	//				isSlow = isslowValue.Bool()
	//			}
	//			fingerprint := getFingerprint(sr.Attributes())
	//			if err := e.recordSpan(td, now, serviceName, fingerprint, isSlow, sr, iss, rs); err != nil {
	//				e.logger.Error("Failed to record span", zap.Error(err))
	//			}
	//		}
	//	}
	//}

	return td, nil
}

func toSize(attributes map[string]interface{}) int64 {
	var size int64 = 0
	for key, value := range attributes {
		size += int64(len(key) + len(fmt.Sprintf("%v", value)))
	}
	return size
}

func (e *statsProc) recordSpan(
	td ptrace.Traces,
	now time.Time,
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

	enrichmentAttributes := e.processEnrichments(map[string]pcommon.Map{
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

	rec := &chqpb.EventStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		ProcessorId: e.id.Name(),
		Count:       1,
		Attributes:  enrichmentAttributes,
		TsHour:      now.Truncate(time.Hour).UnixMilli(),
	}
	e.addSpanExemplar(td, fingerprint)

	bucketpile, err := e.spanStats.Record(now, rec, "", 1, spanSize)
	telemetry.HistogramRecord(e.recordLatency, int64(time.Since(now)))
	if err != nil {
		return err
	}
	e.sendSpanStatsWithExemplars(bucketpile, now)
	return nil
}

func (e *statsProc) sendSpanStatsWithExemplars(bucketpile map[uint64][]*chqpb.EventStats, now time.Time) {
	if bucketpile != nil && len(bucketpile) > 0 {
		for bucketKey, items := range bucketpile {
			itemsWithValidExemplars := items[:0]
			for _, item := range items {
				e.exemplarsMu.RLock()
				exemplar, found := e.traceExemplars[item.Fingerprint]
				e.exemplarsMu.RUnlock()
				if !found {
					continue
				}
				marshalled, err := e.jsonMarshaller.tracesMarshaler.MarshalTraces(exemplar)
				if err != nil {
					continue
				}
				item.Exemplar = marshalled
				itemsWithValidExemplars = append(itemsWithValidExemplars, item)
			}
			if len(itemsWithValidExemplars) > 0 {
				bucketpile[bucketKey] = itemsWithValidExemplars
			} else {
				delete(bucketpile, bucketKey)
			}
		}

		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendSpanStats(context.Background(), now, bucketpile)
	}
}

func (e *statsProc) addSpanExemplar(td ptrace.Traces, fingerprint int64) {
	e.exemplarsMu.RLock()
	_, found := e.traceExemplars[fingerprint]
	e.exemplarsMu.RUnlock()

	if !found {
		copyObj := ptrace.NewTraces()
		td.CopyTo(copyObj)
		// iterate over all 3 levels, and just filter any span records that don't match the fingerprint
		copyObj.ResourceSpans().RemoveIf(func(rsp ptrace.ResourceSpans) bool {
			rsp.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
				foundFp := false
				ss.Spans().RemoveIf(func(sr ptrace.Span) bool {
					if getFingerprint(sr.Attributes()) == fingerprint {
						if !foundFp {
							foundFp = true
							return false
						}
						return true
					}
					return true
				})
				return ss.Spans().Len() == 0
			})
			return rsp.ScopeSpans().Len() == 0
		})
		if copyObj.ResourceSpans().Len() > 0 {
			e.exemplarsMu.Lock()
			e.traceExemplars[fingerprint] = copyObj
			e.exemplarsMu.Unlock()
		}
	}
}

func (e *statsProc) sendSpanStats(ctx context.Context, now time.Time, bucketpile map[uint64][]*chqpb.EventStats) {
	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.EventStats{},
	}
	for _, items := range bucketpile {
		wrapper.Stats = append(wrapper.Stats, items...)
	}

	if err := e.postSpanStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send span stats", zap.Error(err))
	}
	e.logger.Debug("Sent log stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *statsProc) postSpanStats(ctx context.Context, wrapper *chqpb.EventStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(e.statsBatchSize, int64(len(b)))
	e.logger.Debug("Sending span stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Endpoint + "/api/v1/spanstats"
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
