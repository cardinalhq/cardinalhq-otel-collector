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
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/translate"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func (e *fingerprintProcessor) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fingerprint, tMap, level, err := e.logFingerprinter.Fingerprint(lr.Body().AsString())
				if err != nil {
					e.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}
				if len(tMap.Items) > 0 {
					tokenSlice := lr.Attributes().PutEmptySlice(translate.CardinalFieldTokens)
					tokenMap := lr.Attributes().PutEmptyMap(translate.CardinalFieldTokenMap)
					for index, token := range tMap.Items {
						tokenSlice.AppendEmpty().SetStr(token)
						literal := tMap.Get(index)
						tokenMap.PutStr(strconv.Itoa(index), literal)
					}
				}
				lr.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				if lr.SeverityNumber() == plog.SeverityNumberUnspecified {
					lr.SetSeverityText(strings.ToUpper(level))
				}
				lr.Attributes().PutStr(translate.CardinalFieldLevel, level)
			}
		}
	}

	return ld, nil
}
