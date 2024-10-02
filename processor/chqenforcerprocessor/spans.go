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

package chqenforcerprocessor

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"net/http"
	"time"
)

func (e *chqEnforcer) ConsumeSpans(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	now := time.Now()
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		serviceName := getServiceName(rs.Resource().Attributes())
		rs.ScopeSpans().RemoveIf(func(iss ptrace.ScopeSpans) bool {
			iss.Spans().RemoveIf(func(sr ptrace.Span) bool {
				fingerprint := getSpanFingerprint(sr.Attributes())

				statusCode := sr.Status().Code().String()
				isSlow := e.isSpanSlow(sr, uint64(fingerprint))

				if e.pbPhase == chqpb.Phase_POST {
					shouldDrop := e.spanSampler(fingerprint, sr, statusCode, isSlow)
					if shouldDrop {
						return true
					}
				}
				if e.config.DropDecorationAttributes {
					removeAllCardinalFields(sr.Attributes())
				}
				if err := e.recordSpan(now, serviceName, fingerprint, sr, statusCode, isSlow); err != nil {
					e.logger.Error("Failed to record span", zap.Error(err))
				}
				return false
			})
			return iss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})

	if td.ResourceSpans().Len() == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}

	return td, nil
}

func (e *chqEnforcer) isSpanSlow(span ptrace.Span, fingerprint uint64) bool {
	spanDuration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Abs().Milliseconds()
	return e.slowSpanPercentile(fingerprint, float64(spanDuration))
}

func (e *chqEnforcer) slowSpanPercentile(fingerprint uint64, duration float64) bool {
	sketch := e.findSpanSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (e *chqEnforcer) findSpanSketch(fingerprint uint64) *SlidingEstimatorStat {
	e.estimatorLock.Lock()
	defer e.estimatorLock.Unlock()
	sketch, ok := e.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(e.estimatorWindowSize, e.estimatorInterval)
		e.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}

// TODO: Implement the spanSampler function
func (e *chqEnforcer) spanSampler(fingerprint int64, span ptrace.Span, statusCode string, isSlow bool) bool {
	//fingerprintString := fmt.Sprintf("%d", fingerprint)

	rule_match := ""
	//rule_match := e.sampler.Sample(fingerprintString, statusCode, isSlow)
	return rule_match != ""
}

func getSpanFingerprint(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
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

func (e *chqEnforcer) recordSpan(now time.Time, serviceName string, fingerprint int64, span ptrace.Span, statusCode string, isSlow bool) error {
	// spanSize = (size of attributes + top level fields)
	var stringAttributes, spanSize = toAttributesAndSize(span.Attributes().AsRaw())
	spanSize += int64(len(span.TraceID().String()))
	spanSize += int64(len(span.Name()))
	spanSize += int64(len(span.Kind().String()))
	spanSize += int64(len(span.SpanID().String()))

	rec := &chqpb.SpanStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		VendorId:    e.config.Statistics.Vendor,
		Count:       1,
		Tags: map[string]string{
			"statusCode": statusCode,
			"isSlow":     fmt.Sprintf("%v", isSlow),
		},
		Exemplar: &chqpb.SpanExemplar{
			TraceId:  span.TraceID().String(),
			SpanName: span.Name(),
			SpanKind: span.Kind().String(),
			Resource: "",
			SpanTags: stringAttributes,
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

func (e *chqEnforcer) sendSpanStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.SpanStats) {
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

func (e *chqEnforcer) postSpanStats(ctx context.Context, wrapper *chqpb.SpanStatsReport) error {
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
