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
	"github.com/cespare/xxhash/v2"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
	"os"
	"strings"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

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

func (sp *spansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return sp.decorateTraces(td)
}

//TODO: Use this function to get the resource field and add it to span fingerprint
/**
  val resource = scrubResource({
    val ddr = if (columnNames.contains(DD_RESOURCE)) resultSet.getString(DD_RESOURCE) else ""
    if (ddr == null || ddr.isEmpty) {
      if (spanHttpMethod != null && spanHttpMethod.nonEmpty) {
        if (spanHttpRoute != null && spanHttpRoute.nonEmpty) {
          spanHttpMethod + " " + spanHttpRoute
        } else if (spanHttpUrl != null && spanHttpUrl.nonEmpty) {
          spanHttpMethod + " " + spanHttpUrl
        } else {
          spanHttpMethod
        }
      } else {
        ""
      }
    } else ddr
  })
*/
func getSpanFingerprint(sr ptrace.Span, serviceName string) int64 {
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

	// Compute hash on parts
	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}

func (sp *spansProcessor) decorateTraces(td ptrace.Traces) (ptrace.Traces, error) {
	environment := translate.EnvironmentFromEnv()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		snk := string(semconv.ServiceNameKey)
		serviceName, serviceNameExists := rs.Resource().Attributes().Get(snk)

		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				var spanFingerprint int64
				if serviceNameExists {
					spanFingerprint = getSpanFingerprint(span, serviceName.Str())
				} else {
					spanFingerprint = getSpanFingerprint(span, "unknown")
				}

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
