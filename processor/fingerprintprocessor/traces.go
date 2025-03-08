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
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/ottl/functions"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (p *fingerprintProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				spanFingerprint := calculateSpanFingerprint(sr)
				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)

				spanDuration := float64(sr.EndTimestamp().AsTime().Sub(sr.StartTimestamp().AsTime()).Abs().Milliseconds())
				sr.Attributes().PutDouble(translate.CardinalFieldSpanDuration, spanDuration)
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
	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}
