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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
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

func (e *statsExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
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
				e.record(ctx, now, serviceName, fp, filtered, wouldFilter)
			}
		}
	}
	return nil
}

func (e *statsExporter) record(ctx context.Context, now time.Time, serviceName string, fingerprint int64, filtered bool, wouldFilter bool) {
	rec := LogStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Filtered:    filtered,
		WouldFilter: wouldFilter,
		Count:       1,
	}
	bucketpile := e.logstats.Record(now, &rec)
	if bucketpile != nil {
		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendLogStats(ctx, now, bucketpile)
	}
}

func (e *statsExporter) sendLogStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*LogStats) {
	wrapper := LogStatsReport{
		SubmittedAt: now,
		Stats:       []*LogStats{},
	}
	for _, items := range *bucketpile {
		wrapper.Stats = append(wrapper.Stats, items...)
	}

	if err := e.startSend(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send log stats", zap.Error(err))
	}
}

func (e *statsExporter) startSend(ctx context.Context, wrapper LogStatsReport) error {
	b, err := json.Marshal(wrapper)
	if err != nil {
		return err
	}
	endpoint := e.config.Endpoint + "/logstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
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
