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
	"os"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/spantagger"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

type spansProcessor struct {
	logger  *zap.Logger
	podName string
}

func newSpansProcessor(set processor.Settings, _ *Config) (*spansProcessor, error) {
	sp := &spansProcessor{
		logger:  set.Logger,
		podName: os.Getenv("POD_NAME"),
	}

	return sp, nil
}

func getFingerprint(traces ptrace.Traces) (uint64, bool, string) {
	fp, he, err := spantagger.Fingerprint(traces)
	switch err {
	case nil:
		return fp, he, ""
	case spantagger.InconsistentTraceIDsError:
		return 0, he, "InconsistentTraceIDs"
	case spantagger.OrphanedSpanError:
		return 0, he, "OrphanedSpan"
	case spantagger.NoRootError:
		return 0, he, "NoRoot"
	case spantagger.MultipleRootsError:
		return 0, he, "MultipleRoots"
	default:
		return 0, he, "UnknownError"
	}
}

func (sp *spansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	fingerprint, hasError, fpError := getFingerprint(td)
	return sp.decorateTraces(td, fingerprint, hasError, fpError)
}

func (sp *spansProcessor) decorateTraces(td ptrace.Traces, fingerprint uint64, hasError bool, fpError string) (ptrace.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				span.Attributes().PutInt(translate.CardinalFieldFingerprint, int64(fingerprint))
				span.Attributes().PutBool(translate.CardinalFieldTraceHasError, hasError)
				span.Attributes().PutBool(translate.CardinalFieldFingerprintError, span.Status().Code() == ptrace.StatusCodeError)
				if fpError != "" {
					span.Attributes().PutStr(translate.CardinalFieldSpanHasError, fpError)
				}
				if span.ParentSpanID().IsEmpty() {
					span.Attributes().PutBool(translate.CardinalFieldIsRootSpan, true)
				}
				span.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, sp.podName)
			}
		}
	}

	return td, nil
}

func (sp *spansProcessor) Shutdown(context.Context) error {
	return nil
}
