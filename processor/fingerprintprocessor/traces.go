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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"strconv"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/ottl/functions"
	"github.com/cardinalhq/oteltools/pkg/translate"

	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

func (p *fingerprintProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			iss := rs.ScopeSpans().At(j)
			for k := 0; k < iss.Spans().Len(); k++ {
				sr := iss.Spans().At(k)
				spanFingerprint := p.calculateSpanFingerprint(sr)
				sr.Attributes().PutInt(translate.CardinalFieldFingerprint, spanFingerprint)

				spanDuration := float64(sr.EndTimestamp().AsTime().Sub(sr.StartTimestamp().AsTime()).Abs().Milliseconds())
				sr.Attributes().PutDouble("_cardinalhq.span_duration", spanDuration)
			}
		}
	}

	return td, nil
}

func (p *fingerprintProcessor) calculateSpanFingerprint(sr ptrace.Span) int64 {
	var exceptionMessage string
	for i := 0; i < sr.Events().Len(); i++ {
		event := sr.Events().At(i)
		if event.Name() == semconv.ExceptionEventName {
			if exType, found := event.Attributes().Get(string(semconv.ExceptionTypeKey)); found {
				exceptionMessage = exceptionMessage + exType.AsString()
			}
			if exMsg, found := event.Attributes().Get(string(semconv.ExceptionMessageKey)); found {
				if exceptionMessage != "" {
					exceptionMessage = exceptionMessage + " " + exMsg.AsString()
				} else {
					exceptionMessage = exMsg.AsString()
				}
			}
			if exStack, found := event.Attributes().Get(string(semconv.ExceptionStacktraceKey)); found {
				if exceptionMessage != "" {
					exceptionMessage = exceptionMessage + "\n" + exStack.AsString()
				} else {
					exceptionMessage = exStack.AsString()
				}
			}
			break
		}
	}

	var tokenMap pcommon.Map
	var computedFingerprint int64
	if exceptionMessage != "" {
		fingerprint, tMap, _, _, err := p.traceFingerprinter.Fingerprint(exceptionMessage)
		if err == nil {
			tokenMap = p.addTokenMap(tMap, sr.Attributes())
			computedFingerprint = fingerprint
		}
	}

	if tokenMap.Len() == 0 {
		tokenMap = sr.Attributes().PutEmptyMap(translate.CardinalFieldTokenMap)
	}

	fingerprintAttributes := make([]string, 0)
	sanitizedName := functions.ScrubWord(sr.Name())
	tokenMap.PutStr("spanName", sanitizedName)
	fingerprintAttributes = append(fingerprintAttributes, sanitizedName)

	spanKindStr := sr.Kind().String()
	tokenMap.PutStr("spanKind", spanKindStr)
	fingerprintAttributes = append(fingerprintAttributes, spanKindStr)

	statusCodeStr := sr.Status().Code().String()
	tokenMap.PutStr("statusCode", statusCodeStr)
	fingerprintAttributes = append(fingerprintAttributes, statusCodeStr)

	if computedFingerprint != 0 {
		fingerprintAttributes = append(fingerprintAttributes, strconv.FormatInt(computedFingerprint, 10))
	}

	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}
