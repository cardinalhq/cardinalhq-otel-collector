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

package piiredactionprocessor

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func (p *piiRedactionProcessor) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if lr.Body().Type() == pcommon.ValueTypeStr {
					p.sanitizeBodyString(lr)
				} else if lr.Body().Type() == pcommon.ValueTypeMap {
					p.sanitizeBodyMap(lr)
				}
			}
		}
	}

	return ld, nil
}

func (p *piiRedactionProcessor) sanitizeBodyString(lr plog.LogRecord) {
	tokens, err := p.detector.Tokenize(lr.Body().AsString())
	if err != nil {
		p.logger.Debug("Error tokenizing log", zap.Error(err))
		return
	}
	newBody := p.detector.Sanitize(lr.Body().AsString(), tokens)
	lr.Body().SetStr(newBody)
}

func (p *piiRedactionProcessor) sanitizeBodyMap(lr plog.LogRecord) {
	bodyMap := lr.Body().Map()
	bodyMap.Range(func(k string, v pcommon.Value) bool {
		if v.Type() == pcommon.ValueTypeStr {
			tokens, err := p.detector.Tokenize(v.AsString())
			if err != nil {
				p.logger.Debug("Error tokenizing log", zap.Error(err))
				return true
			}
			newValue := p.detector.Sanitize(v.AsString(), tokens)
			bodyMap.PutStr(k, newValue)
		}
		return true
	})
}
