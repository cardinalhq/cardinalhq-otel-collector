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
	"fmt"
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
		tenant := p.getTenant(cid)
		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)
			for k := range sl.LogRecords().Len() {
				lr := sl.LogRecords().At(k)
				fingerprint, levelfromFingerprinter, err := p.addTokenFields(tenant, lr)
				if err != nil {
					p.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}
				lr.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				if lr.SeverityText() == "" || lr.SeverityText() == plog.SeverityNumberUnspecified.String() || lr.SeverityNumber() == plog.SeverityNumberUnspecified {
					lr.SetSeverityText(strings.ToUpper(levelfromFingerprinter))
				}
				lr.Attributes().PutStr(translate.CardinalFieldLevel, lr.SeverityText())
			}
		}
	}

	return ld, nil
}

func (p *fingerprintProcessor) addTokenFields(tenant *tenantState, lr plog.LogRecord) (int64, string, error) {
	fingerprint, tMap, level, js, err := p.logFingerprinter.Fingerprint(lr.Body().AsString())
	if err != nil {
		return 0, "", err
	}

	if replacement, found := tenant.mapstore.Get(fingerprint); found {
		lr.Attributes().PutInt(translate.CardinalFieldFingerprint+"_original", fingerprint)
		fingerprint = replacement
	}

	// add JSON content to the record
	if js == nil {
		js = map[string]any{}
	}
	jsmap := lr.Attributes().PutEmptyMap(translate.CardinalFieldJSON)
	jscm := pcommon.NewMap()
	if err := jscm.FromRaw(js); err != nil {
		p.logger.Debug("Error converting JSON to pdata.Map", zap.Error(err))
	}
	jscm.CopyTo(jsmap)

	if len(tMap.Items) > 0 {
		tokenSlice := lr.Attributes().PutEmptySlice(translate.CardinalFieldTokens)
		tokenMap := lr.Attributes().PutEmptyMap(translate.CardinalFieldTokenMap)
		placeHolderIndexes := make(map[string]int)
		for index, token := range tMap.Items {
			tokenSlice.AppendEmpty().SetStr(token)
			literal := tMap.Get(index)
			if token[0] == '<' && token[len(token)-1] == '>' {
				if _, found := placeHolderIndexes[token]; !found {
					placeHolderIndexes[token] = 0
				} else {
					placeHolderIndexes[token]++
				}
				placeHolderKey := fmt.Sprintf("%s_%d", token, placeHolderIndexes[token])
				tokenMap.PutStr(placeHolderKey, literal)
			} else {
				tokenMap.PutStr(literal, literal)
			}
		}
	}
	return fingerprint, level, nil
}
