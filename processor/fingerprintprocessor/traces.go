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

func (p *fingerprintProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		cid := OrgIdFromResource(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				spanFingerprint := calculateSpanFingerprint(sr)
				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)

				spanDuration := float64(sr.EndTimestamp().AsTime().Sub(sr.StartTimestamp().AsTime()).Abs().Milliseconds())
				sr.Attributes().PutDouble("_cardinalhq.span_duration", spanDuration)

				isSlow := p.isSpanSlow(cid, spanDuration, uint64(spanFingerprint))
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

func (p *fingerprintProcessor) isSpanSlow(cid string, duration float64, fingerprint uint64) bool {
	return p.slowSpanPercentile(cid, fingerprint, duration)
}

func (p *fingerprintProcessor) slowSpanPercentile(cid string, fingerprint uint64, duration float64) bool {
	sketch := p.findSpanSketch(cid, fingerprint)
	sketch.Update(time.Now().UnixMilli(), duration)
	return sketch.GreaterThanThreeStdDev(duration)
}

func (p *fingerprintProcessor) findSpanSketch(cid string, fingerprint uint64) *SlidingEstimatorStat {
	p.tenantLock.Lock()
	tenant, found := p.tenants[cid]
	if !found {
		tenant := tenantState{
			mapstore:   NewMapStore(),
			estimators: make(map[uint64]*SlidingEstimatorStat),
		}
		p.tenants[cid] = tenant
	}
	p.tenantLock.Unlock()

	sketch, ok := tenant.estimators[fingerprint]
	if !ok {
		estimator := NewSlidingEstimatorStat(p.estimatorWindowSize, p.estimatorInterval)
		tenant.estimators[fingerprint] = estimator
		return estimator
	}
	return sketch
}
