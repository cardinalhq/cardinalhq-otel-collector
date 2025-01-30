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

	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (e *fingerprintProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				spanFingerprint := calculateSpanFingerprint(sr)
				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)

				spanDuration := float64(sr.EndTimestamp().AsTime().Sub(sr.StartTimestamp().AsTime()).Abs().Milliseconds())
				sr.Attributes().PutDouble("_cardinalhq.span_duration", spanDuration)

				isSlow := e.isSpanSlow(spanDuration, uint64(spanFingerprint))
				sr.Attributes().PutBool(translate.CardinalFieldSpanIsSlow, isSlow)
			}
		}
	}

	return td, nil
}

func calculateSpanFingerprint(sr ptrace.Span) int64 {
	var fingerprintAttributes []string
	fingerprintAttributes = append(fingerprintAttributes, sr.Name())
	fingerprintAttributes = append(fingerprintAttributes, sr.Kind().String())
	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}

func (c *fingerprintProcessor) isSpanSlow(duration float64, fingerprint uint64) bool {
	return c.slowSpanPercentile(fingerprint, duration)
}

func (c *fingerprintProcessor) slowSpanPercentile(fingerprint uint64, duration float64) bool {
	sketch := c.findSpanSketch(fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (c *fingerprintProcessor) findSpanSketch(fingerprint uint64) *SlidingEstimatorStat {
	sketch, ok := c.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(c.estimatorWindowSize, c.estimatorInterval)
		c.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}
