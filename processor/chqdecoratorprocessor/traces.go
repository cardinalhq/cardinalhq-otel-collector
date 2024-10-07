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

package chqdecoratorprocessor

import (
	"context"
	"fmt"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cespare/xxhash/v2"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

type spansProcessor struct {
	logger  *zap.Logger
	podName string

	finger              fingerprinter.Fingerprinter
	estimatorWindowSize int
	estimatorInterval   int64
	estimatorLock       sync.Mutex
	estimators          map[uint64]*SlidingEstimatorStat

	transformations ottl.Transformations
}

const (
	httpMethod  = "http.request.method"
	httpRoute   = "http.route"
	httpUrlPath = "url.path"
)

func (c *chqDecorator) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return c.decorateTraces(td)
}

func (c *chqDecorator) isSpanSlow(span ptrace.Span, fingerprint uint64) bool {
	spanDuration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Abs().Milliseconds()
	return c.slowSpanPercentile(fingerprint, float64(spanDuration))
}

func (c *chqDecorator) slowSpanPercentile(fingerprint uint64, duration float64) bool {
	sketch := c.findSpanSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (c *chqDecorator) findSpanSketch(fingerprint uint64) *SlidingEstimatorStat {
	sketch, ok := c.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(c.estimatorWindowSize, c.estimatorInterval)
		c.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}

func (c *chqDecorator) getHttpResource(span ptrace.Span) string {
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

	// Add serviceName
	fingerprintAttributes = append(fingerprintAttributes, serviceName)

	// Add spanName
	if spanNameAttr, exists := attrs.Get(translate.CardinalFieldSpanName); exists {
		fingerprintAttributes = append(fingerprintAttributes, spanNameAttr.Str())
	}

	// Add spanKind
	fingerprintAttributes = append(fingerprintAttributes, sr.Kind().String())

	// Add resource
	fingerprintAttributes = append(fingerprintAttributes, httpResource)

	// Compute hash on parts
	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}

func (c *chqDecorator) decorateTraces(td ptrace.Traces) (ptrace.Traces, error) {
	c.Lock()
	defer c.Unlock()

	environment := translate.EnvironmentFromEnv()
	rss := td.ResourceSpans()

	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		// Evaluate resource transformations
		c.traceTransformations.ExecuteResourceSpanTransformations(rs)

		snk := string(semconv.ServiceNameKey)
		serviceName, serviceNameExists := rs.Resource().Attributes().Get(snk)

		ilss := rs.ScopeSpans()

		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			// Evaluate scope transformations
			c.traceTransformations.ExecuteScopeSpanTransformations(ils, rs)

			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// Evaluate scope transformations
				c.traceTransformations.ExecuteSpanTransformations(span, ils, rs)

				httpResource := c.getHttpResource(span)
				if httpResource != "" {
					span.Attributes().PutStr(translate.CardinalFieldResourceName, httpResource)
				}

				var spanFingerprint int64
				if serviceNameExists {
					spanFingerprint = getSpanFingerprint(span, httpResource, serviceName.Str())
				} else {
					spanFingerprint = getSpanFingerprint(span, httpResource, "unknown")
				}
				isSlow := c.isSpanSlow(span, uint64(spanFingerprint))
				span.Attributes().PutBool(translate.CardinalFieldSpanIsSlow, isSlow)

				// Evaluate if we should drop this span, if yes add it to the attributes
				// for the downstream enforcers to drop it.
				shouldDrop := c.shouldDropSpan(serviceName.Str(), spanFingerprint, rs, ils, span)
				span.Attributes().PutBool(translate.CardinalFieldDrop, shouldDrop)

				span.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)
				span.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, c.podName)
				span.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				span.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
			}
		}
	}

	return td, nil
}

func (c *chqDecorator) shouldDropSpan(serviceName string, fingerprint int64, rl ptrace.ResourceSpans, sl ptrace.ScopeSpans, lr ptrace.Span) bool {
	fingerprintString := fmt.Sprintf("%d", fingerprint)
	return c.traceSampler.SampleSpans(serviceName, fingerprintString, rl, sl, lr) != ""
}

func (sp *spansProcessor) Shutdown(context.Context) error {
	return nil
}

func (c *chqDecorator) updateTracesSampling(sc sampler.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating trace sampling config", zap.String("vendor", c.vendor))
	c.traceSampler.UpdateConfig(sc.Traces.Sampling, c.vendor, c.telemetrySettings)
	// ok to ignore the parse error here, because we expect the config to be valid because it got validated
	// before it was saved by the UI.
	transformations, _ := ottl.ParseTransformations(sc.Traces.Transformations, c.logger)
	c.traceTransformations = transformations
}
