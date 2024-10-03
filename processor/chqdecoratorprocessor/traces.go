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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cespare/xxhash/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"

	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
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

	transformations transformations
}

const (
	httpMethod  = "http.request.method"
	httpRoute   = "http.route"
	httpUrlPath = "url.path"
)

func newSpansProcessor(set processor.Settings, c *Config) (*spansProcessor, error) {
	sp := &spansProcessor{
		logger:              set.Logger,
		podName:             os.Getenv("POD_NAME"),
		estimators:          make(map[uint64]*SlidingEstimatorStat),
		estimatorWindowSize: c.TracesConfig.EstimatorWindowSize,
		estimatorInterval:   c.TracesConfig.EstimatorInterval,
	}

	sp.transformations = toTransformations(c.TracesConfig.Transforms, sp.logger)
	sp.finger = fingerprinter.NewFingerprinter()

	return sp, nil
}

func (sp *spansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return sp.decorateTraces(td)
}

func (sp *spansProcessor) isSpanSlow(span ptrace.Span, fingerprint uint64) bool {
	spanDuration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Abs().Milliseconds()
	return sp.slowSpanPercentile(fingerprint, float64(spanDuration))
}

func (sp *spansProcessor) slowSpanPercentile(fingerprint uint64, duration float64) bool {
	sketch := sp.findSpanSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (sp *spansProcessor) findSpanSketch(fingerprint uint64) *SlidingEstimatorStat {
	sp.estimatorLock.Lock()
	defer sp.estimatorLock.Unlock()
	sketch, ok := sp.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(sp.estimatorWindowSize, sp.estimatorInterval)
		sp.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}

func (sp *spansProcessor) evaluateResourceTransformations(rl ptrace.ResourceSpans) {
	transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
	for _, transformation := range sp.transformations.resourceTransformations {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, _, _ = statement.Execute(context.Background(), transformCtx)
			}
		}
	}
}

func (sp *spansProcessor) evaluateScopeTransformations(sl ptrace.ScopeSpans, rl ptrace.ResourceSpans) {
	transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
	for _, transformation := range sp.transformations.scopeTransformations {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, _, _ = statement.Execute(context.Background(), transformCtx)
			}
		}
	}
}

func (sp *spansProcessor) evaluateSpanTransformations(span ptrace.Span, sl ptrace.ScopeSpans, rl ptrace.ResourceSpans) {
	transformCtx := ottlspan.NewTransformContext(span, sl.Scope(), rl.Resource(), sl, rl)
	for _, transformation := range sp.transformations.spanTransformations {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, _, _ = statement.Execute(context.Background(), transformCtx)
			}
		}
	}
}

func (sp *spansProcessor) getHttpResource(span ptrace.Span) string {
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
			urlPathStr, _, err := sp.finger.TokenizeInput(urlPath.Str())
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

func (sp *spansProcessor) decorateTraces(td ptrace.Traces) (ptrace.Traces, error) {
	environment := translate.EnvironmentFromEnv()
	rss := td.ResourceSpans()

	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		// Evaluate resource transformations
		sp.evaluateResourceTransformations(rs)

		snk := string(semconv.ServiceNameKey)
		serviceName, serviceNameExists := rs.Resource().Attributes().Get(snk)

		ilss := rs.ScopeSpans()

		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			// Evaluate scope transformations
			sp.evaluateScopeTransformations(ils, rs)

			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// Evaluate scope transformations
				sp.evaluateSpanTransformations(span, ils, rs)

				httpResource := sp.getHttpResource(span)
				if httpResource != "" {
					span.Attributes().PutStr(translate.CardinalFieldResourceName, httpResource)
				}

				var spanFingerprint int64
				if serviceNameExists {
					spanFingerprint = getSpanFingerprint(span, httpResource, serviceName.Str())
				} else {
					spanFingerprint = getSpanFingerprint(span, httpResource, "unknown")
				}
				isSlow := sp.isSpanSlow(span, uint64(spanFingerprint))
				span.Attributes().PutBool(translate.CardinalFieldSpanIsSlow, isSlow)
				span.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)
				span.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, sp.podName)
				span.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				span.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
			}
		}
	}

	return td, nil
}

func (sp *spansProcessor) Shutdown(context.Context) error {
	return nil
}
