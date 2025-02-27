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

package chqstatsprocessor

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
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

func (p *statsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	ee := authenv.GetEnvironment(ctx, p.idSource)

	// Special case to disable stats gathering if we are running as a SaaS receiver.
	auth := authenv.EnvironmentFromAuth(ctx)
	if auth.CollectorID() != "" {
		return ld, nil
	}

	now := time.Now()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttributes := rl.Resource().Attributes()
		cid := OrgIdFromResource(resourceAttributes)
		tenant := p.getTenant(cid)

		serviceName := getServiceName(resourceAttributes)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fp := getFingerprint(lr.Attributes())
				if err := p.recordLog(tenant, now, ee, serviceName, fp, rl, sl, lr); err != nil {
					p.logger.Error("Failed to record log", zap.Error(err))
				}
			}
		}
	}

	return ld, nil
}

func (p *statsProcessor) recordLog(tenant *Tenant, now time.Time, environment authenv.Environment, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	orgID := OrgIdFromResource(rl.Resource().Attributes())

	if p.enableLogMetrics {
		message := lr.Body().AsString()
		logSize := int64(len(message))

		enrichmentAttributes := p.processEnrichments(orgID,
			map[string]pcommon.Map{
				"resource": rl.Resource().Attributes(),
				"scope":    sl.Scope().Attributes(),
				"log":      lr.Attributes(),
			})

		if lr.SeverityNumber() != plog.SeverityNumberUnspecified {
			enrichmentAttributes = append(enrichmentAttributes, &chqpb.Attribute{
				ContextId:   "log",
				IsAttribute: false,
				Type:        int32(pcommon.ValueTypeStr),
				Key:         "severity",
				Value:       lr.SeverityText(),
			})
		}

		if p.enableLogMetrics {
			err := tenant.logstats.Record(serviceName, fingerprint, p.pbPhase, p.id.Name(), environment.CollectorID(), environment.CustomerID(), enrichmentAttributes, 1, logSize)
			if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
				telemetry.CounterAdd(p.cacheFull, 1)
			}
		}
	}

	p.addLogExemplar(tenant, rl, sl, lr, serviceName, fingerprint)
	telemetry.HistogramRecord(p.recordLatency, int64(time.Since(now)))

	return nil
}

func (p *statsProcessor) addLogExemplar(tenant *Tenant, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord, serviceName string, fingerprint int64) {
	if p.pbPhase == chqpb.Phase_PRE {
		key := p.toExemplarKey(serviceName, fingerprint)

		if tenant.logExemplars.Contains(key) {
			return
		}

		exemplarLd := plog.NewLogs()
		copyRl := exemplarLd.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(copyRl.Resource())
		copySl := copyRl.ScopeLogs().AppendEmpty()
		sl.Scope().CopyTo(copySl.Scope())
		copyLr := copySl.LogRecords().AppendEmpty()
		lr.CopyTo(copyLr)

		marshalled, me := p.jsonMarshaller.logsMarshaler.MarshalLogs(exemplarLd)
		if me != nil {
			return
		}
		tenant.logExemplars.Put(key, marshalled)
	}
}
