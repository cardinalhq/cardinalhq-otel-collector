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

package fingerprintprocessor

import (
	"context"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/oteltools/pkg/ottl/functions"
	"github.com/cardinalhq/oteltools/pkg/translate"

	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

func (p *fingerprintProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		cid := OrgIdFromResource(rs.Resource().Attributes())
		tenant := p.getTenant(cid)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				spanFingerprint := calculateSpanFingerprint(sr)
				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)

				spanDuration := float64(sr.EndTimestamp().AsTime().Sub(sr.StartTimestamp().AsTime()).Abs().Milliseconds())
				sr.Attributes().PutDouble("_cardinalhq.span_duration", spanDuration)

				isSlow := p.isSpanSlow(tenant, spanDuration, uint64(spanFingerprint))
				sr.Attributes().PutBool(translate.CardinalFieldSpanIsSlow, isSlow)
			}
		}
	}

	return td, nil
}

func calculateSpanFingerprint(sr ptrace.Span) int64 {
	var fingerprintAttributes []string
	sanitizedName := functions.ScrubWord(sr.Name())
	fingerprintAttributes = append(fingerprintAttributes, sanitizedName)
	fingerprintAttributes = append(fingerprintAttributes, sr.Kind().String())
	fingerprintAttributes = append(fingerprintAttributes, sr.Status().Code().String())

	for i := 0; i < sr.Events().Len(); i++ {
		event := sr.Events().At(i)
		if event.Name() == semconv.ExceptionEventName {
			if exType, found := event.Attributes().Get(string(semconv.ExceptionTypeKey)); found {
				fingerprintAttributes = append(fingerprintAttributes, exType.AsString())
			}
			if exMsg, found := event.Attributes().Get(string(semconv.ExceptionMessageKey)); found {
				fingerprintAttributes = append(fingerprintAttributes, exMsg.AsString())
			}
			if exStack, found := event.Attributes().Get(string(semconv.ExceptionStacktraceKey)); found {
				fingerprintAttributes = append(fingerprintAttributes, exStack.AsString())
			}
		}
	}

	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}

func (p *fingerprintProcessor) isSpanSlow(tenant *tenantState, duration float64, fingerprint uint64) bool {
	return p.slowSpanPercentile(tenant, fingerprint, duration)
}

func (p *fingerprintProcessor) slowSpanPercentile(tenant *tenantState, fingerprint uint64, duration float64) bool {
	sketch := p.findSpanSketch(tenant, fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (p *fingerprintProcessor) findSpanSketch(tenant *tenantState, fingerprint uint64) *SlidingEstimatorStat {
	sketch, ok := tenant.estimators.Load(fingerprint)
	if !ok {
		estimator := NewSlidingEstimatorStat(p.estimatorWindowSize, p.estimatorInterval)
		tenant.estimators.Store(fingerprint, estimator)
		return estimator
	}
	return sketch
}
