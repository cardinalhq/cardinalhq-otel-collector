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
	"context"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

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

	// TODO add a short-circuit at each level to skip processing if no transformations are defined for that level.

	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		transformCtx := ottlresource.NewTransformContext(rs.Resource(), rs)
		e.traceTransformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx)
		if _, found := rs.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
			return true
		}
		serviceName := getServiceName(rs.Resource().Attributes())
		rs.ScopeSpans().RemoveIf(func(iss ptrace.ScopeSpans) bool {
			transformCtx := ottlscope.NewTransformContext(iss.Scope(), rs.Resource(), rs)
			e.traceTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx)
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
				transformCtx := ottlspan.NewTransformContext(sr, iss.Scope(), rs.Resource(), iss, rs)
				e.traceTransformations.ExecuteSpanTransforms(e.ottlProcessed, transformCtx)
				_, found := sr.Attributes().Get(translate.CardinalFieldDropMarker)
				return found
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

func (e *pitbull) updateTraceTransformations(sc ottl.SamplerConfig) {
	e.Lock()
	defer e.Unlock()
	e.logger.Info("Updating trace transformations", zap.Int("num_decorators", len(sc.Spans.Decorators)))
	newTransformations := ottl.NewTransformations(e.logger)

	for _, decorator := range sc.Spans.Decorators {
		if decorator.ProcessorID != e.id.String() {
			continue
		}
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
