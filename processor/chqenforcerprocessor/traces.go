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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"net/http"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func (e *chqEnforcer) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	e.Lock()
	defer e.Unlock()

	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	now := time.Now()
	transformations := e.traceTransformations

	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		resourceRulesMatched := e.getSlice(rs.Resource().Attributes(), translate.CardinalFieldRulesMatched)
		if resourceRulesMatched.Len() > 0 {
			transformCtx := ottlresource.NewTransformContext(rs.Resource(), rs)
			transformations.ExecuteResourceTransforms(transformCtx, e.vendor, resourceRulesMatched)
		}

		if e.sliceContains(rs.Resource().Attributes(), translate.CardinalFieldDropForVendor, e.vendor) {
			return true
		}

		serviceName := getServiceName(rs.Resource().Attributes())
		rs.ScopeSpans().RemoveIf(func(iss ptrace.ScopeSpans) bool {
			scopeRulesMatched := e.getSlice(iss.Scope().Attributes(), translate.CardinalFieldRulesMatched)
			if scopeRulesMatched.Len() > 0 {
				transformCtx := ottlscope.NewTransformContext(iss.Scope(), rs.Resource(), rs)
				e.logTransformations.ExecuteScopeTransforms(transformCtx, e.vendor, scopeRulesMatched)
			}

			if e.sliceContains(iss.Scope().Attributes(), translate.CardinalFieldDropForVendor, e.vendor) {
				return true
			}

			iss.Spans().RemoveIf(func(sr ptrace.Span) bool {
				fingerprint := getSpanFingerprint(sr.Attributes())
				logRulesMatched := e.getSlice(sr.Attributes(), translate.CardinalFieldRulesMatched)
				if logRulesMatched.Len() > 0 {
					transformCtx := ottlspan.NewTransformContext(sr, iss.Scope(), rs.Resource(), iss, rs)
					e.logTransformations.ExecuteSpanTransforms(transformCtx, e.vendor, logRulesMatched)
				}
				if e.sliceContains(sr.Attributes(), translate.CardinalFieldDropForVendor, e.vendor) {
					return true
				}

				// isSlow is an attribute of the span, but is a cardinal field so will be removed at this point.
				// we want cardinal fields to be removed because we don't want to account for them in the size of the span.
				// being sent to the stats backend.
				var isSlow = false
				if isSlowSpan, exists := sr.Attributes().Get(translate.CardinalFieldSpanIsSlow); exists {
					isSlow = isSlowSpan.Bool()
				}

				if e.config.DropDecorationAttributes {
					removeAllCardinalFields(sr.Attributes())
				}

				if err := e.recordSpan(now, serviceName, fingerprint, isSlow, sr, iss, rs); err != nil {
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

func (e *chqEnforcer) shouldDropSpan(l pcommon.Map) bool {
	fnk := translate.CardinalFieldDropForVendor
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Bool()
	}
	return false
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

func getOrElse(m map[string]string, key string, defaultValue string) string {
	// Check if the key exists in the map
	if value, ok := m[key]; ok {
		return value
	}
	// If the key does not exist, return the default value
	return defaultValue
}

func (e *chqEnforcer) recordSpan(now time.Time,
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

	// Derive tags from e.config.LogsConfig.StatsEnrichments based on the contextId, and then add tags to the SpanStats Tags Map
	tagsToEnrich := map[string]string{
		"span.status_code": span.Status().Code().String(),
		"span.is_slow":     fmt.Sprintf("%v", isSlow),
		"span.kind":        span.Kind().String(),
	}

	otherTags := e.processEnrichments(e.config.TracesConfig.StatsEnrichments, map[string]pcommon.Map{
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

func (c *chqEnforcer) updateTraceTransformations(sc sampler.SamplerConfig) {
	c.Lock()
	defer c.Unlock()

	c.logger.Info("Updating trace transformations config", zap.String("vendor", c.vendor))
	for _, decorator := range sc.Traces.Enforcers {
		if decorator.VendorId == c.vendor {
			transformations, err := ottl.ParseTransformations(decorator, c.logger)
			if err != nil {
				c.logger.Error("Error parsing log transformation", zap.Error(err))
			} else {
				c.traceTransformations = ottl.MergeWith(c.traceTransformations, transformations)
			}
		}
	}
}
