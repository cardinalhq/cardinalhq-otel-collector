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
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
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

func (p *fingerprintProcessor) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		cid := OrgIdFromResource(rl.Resource().Attributes())
		fpr := p.GetOrCreateFingerprinter(cid)
		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)
			for k := range sl.LogRecords().Len() {
				lr := sl.LogRecords().At(k)
				fingerprint, levelfromFingerprinter, err := p.addTokenFields(fpr, lr)
				if err != nil {
					p.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}
				lr.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				if lr.SeverityNumber() != plog.SeverityNumberUnspecified {
					lr.SetSeverityText(SeverityNumberToText(lr.SeverityNumber()))
				} else if lr.SeverityText() == "" || lr.SeverityText() == plog.SeverityNumberUnspecified.String() {
					lr.SetSeverityText(strings.ToUpper(levelfromFingerprinter))
				}
				lr.Attributes().PutStr(translate.CardinalFieldLevel, lr.SeverityText())
			}
		}
	}

	return ld, nil
}

func SeverityNumberToText(severityNumber plog.SeverityNumber) string {
	switch {
	case severityNumber >= 1 && severityNumber <= 4:
		return "TRACE"
	case severityNumber >= 5 && severityNumber <= 8:
		return "DEBUG"
	case severityNumber >= 9 && severityNumber <= 12:
		return "INFO"
	case severityNumber >= 13 && severityNumber <= 16:
		return "WARN"
	case severityNumber >= 17 && severityNumber <= 20:
		return "ERROR"
	case severityNumber >= 21 && severityNumber <= 24:
		return "FATAL"
	default:
		return "UNSPECIFIED"
	}
}

func (p *fingerprintProcessor) addTokenFields(fpr fingerprinter.Fingerprinter, lr plog.LogRecord) (int64, string, error) {
	fingerprint, level, js, err := fpr.Fingerprint(lr.Body().AsString())
	if err != nil {
		return 0, "", err
	}

	attributes := lr.Attributes()

	// add JSON content to the record
	if js == nil {
		js = map[string]any{}
	}
	jsmap := attributes.PutEmptyMap(translate.CardinalFieldJSON)
	jscm := pcommon.NewMap()
	if err := jscm.FromRaw(js); err != nil {
		p.logger.Debug("Error converting JSON to pdata.Map", zap.Error(err))
	}
	jscm.CopyTo(jsmap)

	return fingerprint, level, nil
}
