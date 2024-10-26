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

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
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
				if err := e.recordLog(now, serviceName, fp, rl, sl, lr); err != nil {
					e.logger.Error("Failed to record log", zap.Error(err))
				}
			}
		}
	}

	return ld, nil
}

func (e *statsProc) recordLog(now time.Time, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	message := lr.Body().AsString()
	logSize := int64(len(message))

	// Derive tags from e.config.LogsConfig.StatsEnrichments based on the contextId, and then add tags to the LogStats.Tags Map

	enrichmentAttributes := e.processEnrichments(map[string]pcommon.Map{
		"resource": rl.Resource().Attributes(),
		"scope":    sl.Scope().Attributes(),
		"log":      lr.Attributes(),
	})
	severityAttribute := toAttribute("log", "severity", pcommon.NewValueStr(lr.SeverityText()), false)
	enrichmentAttributes = append(enrichmentAttributes, severityAttribute)

	rec := &chqpb.LogStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		VendorId:    e.config.Statistics.Vendor,
		Count:       1,
		LogSize:     logSize,
		Exemplar: &chqpb.LogExemplar{
			Attributes: ToAttributes(rl.Resource(), sl.Scope(), lr.Attributes()),
			Exemplar:   message,
		},
		Attributes: enrichmentAttributes,
	}
	bucketpile, err := e.logstats.Record(now, rec, "", 1, logSize)
	if err != nil {
		return err
	}
	if bucketpile != nil {
		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendLogStats(context.Background(), now, bucketpile)
	}
	return nil
}

func (e *statsProc) sendLogStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.LogStats) {
	wrapper := &chqpb.LogStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.LogStats{},
	}
	for _, items := range *bucketpile {
		wrapper.Stats = append(wrapper.Stats, items...)
	}

	if err := e.postLogStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send log stats", zap.Error(err))
	}
	e.logger.Info("Sent log stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *statsProc) postLogStats(ctx context.Context, wrapper *chqpb.LogStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	e.logger.Info("Sending log stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Statistics.Endpoint + "/api/v1/logstats"
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
