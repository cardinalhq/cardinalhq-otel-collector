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

package pitbullprocessor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

const (
	httpMethod  = "http.request.method"
	httpRoute   = "http.route"
	httpUrlPath = "url.path"
)

func (e *pitbull) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	e.Lock()
	defer e.Unlock()

	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	environment := translate.EnvironmentFromEnv()

	now := time.Now()
	transformations := e.traceTransformations
	emptySlice := pcommon.NewSlice() // TODO remove when we fully remove chq(decorator|enforcer) processor

	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		transformCtx := ottlresource.NewTransformContext(rs.Resource(), rs)
		transformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
		if _, found := rs.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
			return true
		}

		serviceName := getServiceName(rs.Resource().Attributes())
		rs.ScopeSpans().RemoveIf(func(iss ptrace.ScopeSpans) bool {
			transformCtx := ottlscope.NewTransformContext(iss.Scope(), rs.Resource(), rs)
			e.logTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
			if _, found := iss.Scope().Attributes().Get(translate.CardinalFieldDropMarker); found {
				return true
			}

			iss.Spans().RemoveIf(func(sr ptrace.Span) bool {
				httpResource := e.getHttpResource(sr)
				if httpResource != "" {
					sr.Attributes().PutStr(translate.CardinalFieldResourceName, httpResource)
				}

				spanFingerprint := getSpanFingerprint(sr, httpResource, serviceName)
				isSlow := e.isSpanSlow(sr, uint64(spanFingerprint))
				sr.Attributes().PutBool(translate.CardinalFieldSpanIsSlow, isSlow)

				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)
				sr.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, e.podName)
				sr.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				sr.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())

				transformCtx := ottlspan.NewTransformContext(sr, iss.Scope(), rs.Resource(), iss, rs)
				e.logTransformations.ExecuteSpanTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
				if _, found := sr.Attributes().Get(translate.CardinalFieldDropMarker); found {
					return true
				}

				if e.config.DropDecorationAttributes {
					removeAllCardinalFields(sr.Attributes())
				}

				if err := e.recordSpan(now, serviceName, spanFingerprint, isSlow, sr, iss, rs); err != nil {
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

func (c *pitbull) isSpanSlow(span ptrace.Span, fingerprint uint64) bool {
	spanDuration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Abs().Milliseconds()
	return c.slowSpanPercentile(fingerprint, float64(spanDuration))
}

func (c *pitbull) slowSpanPercentile(fingerprint uint64, duration float64) bool {
	sketch := c.findSpanSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (c *pitbull) findSpanSketch(fingerprint uint64) *SlidingEstimatorStat {
	sketch, ok := c.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(c.estimatorWindowSize, c.estimatorInterval)
		c.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}

func (c *pitbull) getHttpResource(span ptrace.Span) string {
	attrs := span.Attributes()
	var resourceKeys []string

	//Reference: https://opentelemetry.io/docs/specs/semconv/http/http-spans/
	if method, methodExists := attrs.Get(httpMethod); methodExists {
		resourceKeys = append(resourceKeys, method.Str())
	}

	if route, routeExists := attrs.Get(httpRoute); routeExists {
		resourceKeys = append(resourceKeys, route.Str())
	} else {
		if urlPath, urlPathExists := attrs.Get(httpUrlPath); urlPathExists {
			urlPathStr, _, err := c.traceFingerprinter.TokenizeInput(urlPath.Str())
			if err == nil {
				resourceKeys = append(resourceKeys, urlPathStr)
			}
		}
	}

	if len(resourceKeys) > 0 {
		return strings.Join(resourceKeys, " ")
	}
	return ""
}

func getSpanFingerprint(sr ptrace.Span, httpResource string, serviceName string) int64 {
	attrs := sr.Attributes()
	var fingerprintAttributes []string

	fingerprintAttributes = append(fingerprintAttributes, serviceName)
	if spanNameAttr, exists := attrs.Get(translate.CardinalFieldSpanName); exists {
		fingerprintAttributes = append(fingerprintAttributes, spanNameAttr.Str())
	}
	fingerprintAttributes = append(fingerprintAttributes, sr.Kind().String())
	fingerprintAttributes = append(fingerprintAttributes, httpResource)

	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
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

func (e *pitbull) recordSpan(now time.Time,
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

func (e *pitbull) sendSpanStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.SpanStats) {
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

func (e *pitbull) postSpanStats(ctx context.Context, wrapper *chqpb.SpanStatsReport) error {
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

func (e *pitbull) updateTraceTransformations(sc ottl.SamplerConfig) {
	e.Lock()
	defer e.Unlock()
	e.logger.Info("Updating trace transformations", zap.Int("num_decorators", len(sc.Spans.Decorators)))
	newTransformations := ottl.NewTransformations(e.logger)

	for _, decorator := range sc.Spans.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, e.logger)
		if err != nil {
			e.logger.Error("Error parsing traces transformation", zap.Error(err))
			continue
		}
		newTransformations = ottl.MergeWith(newTransformations, transformations)
	}

	oldTransformation := e.traceTransformations
	e.traceTransformations = newTransformations
	oldTransformation.Stop()
}
