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

package chqexemplarprocessor

import (
	"context"
	"strconv"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func getFingerprint(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}

func (p *exemplarProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if !p.config.Reporting.Logs.Enabled {
		return ld, nil
	}

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttributes := rl.Resource().Attributes()
		cid := orgIdFromResource(resourceAttributes)
		tenant := p.getTenant(cid)

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fingerprint := getFingerprint(lr.Attributes())
				p.addLogExemplar(tenant, rl, sl, lr, fingerprint)
			}
		}
	}

	return ld, nil
}

func (p *exemplarProcessor) addLogExemplar(tenant *Tenant, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord, fingerprint int64) {
	keys, exemplarKey := computeExemplarKey(rl.Resource(), []string{translate.CardinalFieldFingerprint, strconv.FormatInt(fingerprint, 10)})
	if tenant.logCache.Contains(exemplarKey) {
		return
	}

	exemplarRecord := plog.NewLogs()
	copyRl := exemplarRecord.ResourceLogs().AppendEmpty()
	rl.Resource().CopyTo(copyRl.Resource())
	copySl := copyRl.ScopeLogs().AppendEmpty()
	sl.Scope().CopyTo(copySl.Scope())
	copyLr := copySl.LogRecords().AppendEmpty()
	lr.CopyTo(copyLr)

	tenant.logCache.Put(exemplarKey, keys, exemplarRecord)
}
