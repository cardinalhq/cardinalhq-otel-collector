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

package chqstatsexporter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func getFingerprint(l pcommon.Map) int64 {
	fnk := "_cardinalhq.fingerprint"
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}

func getFiltered(l pcommon.Map) bool {
	fnk := "_cardinalhq.filtered"
	if filteredField, found := l.Get(fnk); found {
		return filteredField.Bool()
	}
	return false
}

func getWouldFilter(l pcommon.Map) bool {
	fnk := "_cardinalhq.would_filter"
	if wouldFilterField, found := l.Get(fnk); found {
		return wouldFilterField.Bool()
	}
	return false
}

func logBoolsToPhase(filtered, wouldFilter bool) chqpb.Phase {
	if filtered {
		return chqpb.Phase_FILTERED
	}
	if wouldFilter {
		return chqpb.Phase_DRY_RUN_FILTERED
	}
	return chqpb.Phase_PASSTHROUGH
}

func (e *statsExporter) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	now := time.Now()
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		rAttr := pcommon.NewMap()
		rl.Resource().Attributes().CopyTo(rAttr)
		serviceName := getServiceName(rAttr)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			ill := rl.ScopeLogs().At(j)
			sAttr := pcommon.NewMap()
			ill.Scope().Attributes().CopyTo(sAttr)
			for k := 0; k < ill.LogRecords().Len(); k++ {
				l := ill.LogRecords().At(k)
				lAttr := pcommon.NewMap()
				l.Attributes().CopyTo(lAttr)
				fp := getFingerprint(lAttr)
				filtered := getFiltered(lAttr)
				wouldFilter := getWouldFilter(lAttr)
				phase := logBoolsToPhase(filtered, wouldFilter)
				if err := e.recordLog(now, serviceName, fp, phase, l.Body().AsString()); err != nil {
					e.logger.Error("Failed to record log", zap.Error(err))
				}
			}
		}
	}
	return nil
}

func (e *statsExporter) recordLog(now time.Time, serviceName string, fingerprint int64, phase chqpb.Phase, message string) error {
	logSize := int64(len(message))
	rec := chqpb.LogStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       phase,
		Count:       1,
		LogSize:     logSize,
		Exemplar:    message,
	}
	bucketpile, err := e.logstats.Record(now, &rec, "", 1, logSize)
	if err != nil {
		return err
	}
	if bucketpile != nil {
		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendLogStats(context.Background(), now, bucketpile)
	}
	return nil
}

func (e *statsExporter) sendLogStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.LogStats) {
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
}

func (e *statsExporter) postLogStats(ctx context.Context, wrapper *chqpb.LogStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	endpoint := e.config.Endpoint + "/api/v1/logstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("x-cardinalhq-api-key", string(e.config.APIKey))

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
