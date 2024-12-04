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

func (e *statsProc) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	now := time.Now()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		serviceName := getServiceName(rl.Resource().Attributes())
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fp := getFingerprint(lr.Attributes())
				if err := e.recordLog(ld, now, serviceName, fp, rl, sl, lr); err != nil {
					e.logger.Error("Failed to record log", zap.Error(err))
				}
			}
		}
	}

	return ld, nil
}

func (e *statsProc) recordLog(ld plog.Logs, now time.Time, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	message := lr.Body().AsString()
	logSize := int64(len(message))

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
	}

	e.addLogExemplar(ld, rec)

	bucketpile, err := e.logstats.Record(now, rec, "", 1, logSize)
	if err != nil {
		return err
	}
	e.sendLogStatsWithExemplars(bucketpile, now)
	telemetry.HistogramRecord(e.recordLatency, int64(time.Since(now)))
	return nil
}

func (l *LogExemplarState) tidy(serviceName string, fingerprint int64) plog.Logs {
	if l.Cleaned {
		return l.Exemplar
	}

	copyObj := plog.NewLogs()
	l.Exemplar.CopyTo(copyObj)
	// iterate over all 3 levels, and just filter any log records that don't match the fingerprint
	copyObj.ResourceLogs().RemoveIf(func(rsp plog.ResourceLogs) bool {
		incomingServiceName := getServiceName(rsp.Resource().Attributes())
		if incomingServiceName != serviceName {
			return true
		}
		rsp.ScopeLogs().RemoveIf(func(ss plog.ScopeLogs) bool {
			foundFp := false // make sure we only add one record matching the fingerprint.
			ss.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				if getFingerprint(lr.Attributes()) == fingerprint {
					if !foundFp {
						foundFp = true
						return false
					}
					return true
				}
				return true
			})
			return ss.LogRecords().Len() == 0
		})
		return rsp.ScopeLogs().Len() == 0
	})
	return copyObj
}

func (e *statsProc) sendLogStatsWithExemplars(bucketpile *map[uint64][]*chqpb.EventStats, now time.Time) {
	if bucketpile != nil && len(*bucketpile) > 0 {
		e.exemplarsMu.Lock()
		defer e.exemplarsMu.Unlock()

		for bucketKey, eventsStatsList := range *bucketpile {
			var itemsWithValidExemplars []*chqpb.EventStats
			for _, eventStats := range eventsStatsList {
				exemplar, found := e.logExemplars[eventStats.Key()]
				if !found {
					continue
				}

				cleaned := exemplar.tidy(eventStats.ServiceName, eventStats.Fingerprint)
				marshalled, err := e.jsonMarshaller.logsMarshaler.MarshalLogs(cleaned)
				if string(marshalled) == "{}" {
					e.logger.Error("Empty exemplar found", zap.Int64("fingerprint", eventStats.Fingerprint), zap.String("serviceName", eventStats.ServiceName))
				}
				if err != nil {
					continue
				}
				eventStats.Exemplar = marshalled
				itemsWithValidExemplars = append(itemsWithValidExemplars, eventStats)
			}
			if len(itemsWithValidExemplars) > 0 {
				(*bucketpile)[bucketKey] = itemsWithValidExemplars
			} else {
				delete(*bucketpile, bucketKey)
			}
		}

		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendLogStats(context.Background(), now, bucketpile)
	}
}

func (e *statsProc) addLogExemplar(ld plog.Logs, stat *chqpb.EventStats) {
	e.exemplarsMu.Lock()
	defer e.exemplarsMu.Unlock()
	key := stat.Key()
	if _, found := e.logExemplars[key]; !found {
		e.logExemplars[key] = LogExemplarState{
			Exemplar:  ld,
			Cleaned:   false,
			CleanedAt: time.Now(),
		}
	}
	nle := e.logExemplars[key]
	if nle.Cleaned && time.Since(nle.CleanedAt) > 5*time.Minute {
		nle.Exemplar = ld
		nle.Cleaned = false
		nle.CleanedAt = time.Now()
	}
}

func (e *statsProc) sendLogStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.EventStats) {
	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.EventStats{},
	}
	for _, items := range *bucketpile {
		wrapper.Stats = append(wrapper.Stats, items...)
	}

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
