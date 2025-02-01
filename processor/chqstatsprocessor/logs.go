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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

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

func (e *statsProc) ConsumeLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	var ee translate.Environment
	if e.idsFromEnv {
		ee = translate.EnvironmentFromEnv()
	} else {
		ee = translate.EnvironmentFromAuth(ctx)
	}

	now := time.Now()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttributes := rl.Resource().Attributes()
		globalEntityMap := e.logsEntityCache.ProvisionResourceAttributes(resourceAttributes)

		serviceName := getServiceName(resourceAttributes)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fp := getFingerprint(lr.Attributes())
				e.logsEntityCache.ProvisionRecordAttributes(globalEntityMap, lr.Attributes())
				if err := e.recordLog(now, ee, serviceName, fp, rl, sl, lr); err != nil {
					e.logger.Error("Failed to record log", zap.Error(err))
				}
			}
		}
	}

	return ld, nil
}

var reportLogMetricsOnce sync.Once

func (e *statsProc) recordLog(now time.Time, environment translate.Environment, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	if e.enableLogMetrics {
		reportLogMetricsOnce.Do(func() {
			e.logger.Info("**************************************** Log metrics are enabled")
		})
		message := lr.Body().AsString()
		logSize := int64(len(message))

		enrichmentAttributes := e.processEnrichments(map[string]pcommon.Map{
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

		if e.enableLogMetrics {
			err := e.logstats.Record(serviceName, fingerprint, e.pbPhase, e.id.Name(), environment.CollectorID(), environment.CustomerID(), enrichmentAttributes, 1, logSize)
			if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
				telemetry.CounterAdd(e.cacheFull, 1)
			}
		}
	}

	e.addLogExemplar(rl, sl, lr, serviceName, fingerprint)
	telemetry.HistogramRecord(e.recordLatency, int64(time.Since(now)))

	return nil
}

func (e *statsProc) addLogExemplar(rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord, serviceName string, fingerprint int64) {
	if e.pbPhase == chqpb.Phase_PRE {
		key := e.toExemplarKey(serviceName, fingerprint)

		if e.logExemplars.Contains(key) {
			return
		}

		exemplarLd := plog.NewLogs()
		copyRl := exemplarLd.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(copyRl.Resource())
		copySl := copyRl.ScopeLogs().AppendEmpty()
		sl.Scope().CopyTo(copySl.Scope())
		copyLr := copySl.LogRecords().AppendEmpty()
		lr.CopyTo(copyLr)

		marshalled, me := e.jsonMarshaller.logsMarshaler.MarshalLogs(exemplarLd)
		if me != nil {
			return
		}
		e.logExemplars.Put(key, marshalled)
	}
}

func (e *statsProc) sendLogStats(statsList []*chqpb.EventStats) {
	for _, stat := range statsList {
		key := e.toExemplarKey(stat.ServiceName, stat.Fingerprint)
		exemplarBytes, found := e.logExemplars.Get(key)
		if !found {
			continue
		}
		stat.Exemplar = exemplarBytes.([]byte)
	}

	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Stats:       statsList}

	if err := e.postLogStats(context.Background(), wrapper); err != nil {
		e.logger.Error("Failed to send log stats", zap.Error(err))
	}
}

func (e *statsProc) postLogStats(ctx context.Context, wrapper *chqpb.EventStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(e.statsBatchSize, int64(len(b)))
	endpoint := e.config.Endpoint + "/api/v1/logstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			e.logger.Error("Failed to close response body", zap.Error(err))
		}
	}(resp.Body)
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send log stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
