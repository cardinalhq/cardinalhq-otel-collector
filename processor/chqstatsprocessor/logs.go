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

package chqstatsprocessor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
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
		serviceName := getServiceName(rl.Resource().Attributes())
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fp := getFingerprint(lr.Attributes())
				if err := e.recordLog(now, ee, serviceName, fp, rl, sl, lr); err != nil {
					e.logger.Error("Failed to record log", zap.Error(err))
				}
			}
		}
	}

	return ld, nil
}

func (e *statsProc) recordLog(now time.Time, environment translate.Environment, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	message := lr.Body().AsString()
	logSize := int64(len(message))

	// TODO need to actually use environment here to record stats

	// Derive tags from e.config.LogsConfig.StatsEnrichments based on the contextId, and then add tags to the LogStats.Tags Map

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

	rec := &chqpb.EventStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		ProcessorId: e.id.Name(),
		Count:       1,
		Size:        logSize,
		Attributes:  enrichmentAttributes,
		TsHour:      now.Truncate(time.Hour).UnixMilli(),
		CollectorId: environment.CollectorID(),
		CustomerId:  environment.CustomerID(),
	}

	e.addLogExemplar(rl, sl, lr, fingerprint)

	bucketpile, err := e.logstats.Record(rec, now)
	if err != nil {
		return err
	}
	e.sendLogStatsWithExemplars(bucketpile, now)
	telemetry.HistogramRecord(e.recordLatency, int64(time.Since(now)))
	return nil
}

func (e *statsProc) sendLogStatsWithExemplars(bucketpile []*chqpb.EventStats, now time.Time) {
	if len(bucketpile) > 0 {
		e.exemplarsMu.Lock()
		defer e.exemplarsMu.Unlock()

		var itemsWithValidExemplars []*chqpb.EventStats
		for _, item := range bucketpile {
			exemplar, found := e.logExemplars[item.Fingerprint]
			if !found {
				continue
			}

			marshalled, err := e.jsonMarshaller.logsMarshaler.MarshalLogs(exemplar)
			if err != nil {
				continue
			}
			item.Exemplar = marshalled
			itemsWithValidExemplars = append(itemsWithValidExemplars, item)
		}

		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendLogStats(context.Background(), now, itemsWithValidExemplars)
	}
}

func (e *statsProc) addLogExemplar(rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord, fingerprint int64) {
	e.exemplarsMu.Lock()
	defer e.exemplarsMu.Unlock()
	if _, found := e.logExemplars[fingerprint]; !found {
		exemplarLd := plog.NewLogs()
		copyRl := exemplarLd.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(copyRl.Resource())
		copySl := copyRl.ScopeLogs().AppendEmpty()
		sl.Scope().CopyTo(copySl.Scope())
		copyLr := copySl.LogRecords().AppendEmpty()
		lr.CopyTo(copyLr)
		e.logExemplars[fingerprint] = exemplarLd
	}
}

func (e *statsProc) sendLogStats(ctx context.Context, now time.Time, statsList []*chqpb.EventStats) {
	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.EventStats{},
	}
	wrapper.Stats = append(wrapper.Stats, statsList...)

	if err := e.postLogStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send log stats", zap.Error(err))
	}
	e.logger.Debug("Sent log stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *statsProc) postLogStats(ctx context.Context, wrapper *chqpb.EventStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(e.statsBatchSize, int64(len(b)))
	endpoint := e.config.Endpoint + "/api/v1/logstats"
	e.logger.Debug("Sending log stats", zap.String("endpoint", endpoint), zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send log stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
