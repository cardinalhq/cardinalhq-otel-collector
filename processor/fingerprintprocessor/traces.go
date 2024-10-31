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

package fingerprintprocessor

import (
	"context"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

const (
	httpMethod  = "http.request.method"
	httpRoute   = "http.route"
	httpUrlPath = "url.path"
)

func (e *aggregationProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		serviceName := getServiceName(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				httpResource := e.getHttpResource(sr)
				if httpResource != "" {
					sr.Attributes().PutStr(translate.CardinalFieldResourceName, httpResource)
				}
				spanFingerprint := calculateSpanFingerprint(sr, httpResource, serviceName)
				isSlow := e.isSpanSlow(sr, uint64(spanFingerprint))
				sr.Attributes().PutBool(translate.CardinalFieldSpanIsSlow, isSlow)
				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)
			}
		}
	}

	return td, nil
}

func (c *aggregationProcessor) isSpanSlow(span ptrace.Span, fingerprint uint64) bool {
	spanDuration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Abs().Milliseconds()
	return c.slowSpanPercentile(fingerprint, float64(spanDuration))
}

func (c *aggregationProcessor) slowSpanPercentile(fingerprint uint64, duration float64) bool {
	sketch := c.findSpanSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (c *aggregationProcessor) findSpanSketch(fingerprint uint64) *SlidingEstimatorStat {
	sketch, ok := c.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(c.estimatorWindowSize, c.estimatorInterval)
		c.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}

func (c *aggregationProcessor) getHttpResource(span ptrace.Span) string {
	attrs := span.Attributes()
	var resourceKeys []string

	//Reference: https://opentelemetry.io/docs/specs/semconv/http/http-spans/
	if method, exists := attrs.Get(httpMethod); exists {
		resourceKeys = append(resourceKeys, method.Str())
	}

	if route, exists := attrs.Get(httpRoute); exists {
		resourceKeys = append(resourceKeys, route.Str())
	} else {
		if urlPath, exists := attrs.Get(httpUrlPath); exists {
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

func calculateSpanFingerprint(sr ptrace.Span, httpResource string, serviceName string) int64 {
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
