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
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

//
// Logs
//

func (p *statsProcessor) sendLogStats(cid string) func([]*chqpb.EventStats) {
	return func(stats []*chqpb.EventStats) {
		p.sendLogStatsFor(cid, stats)
	}
}

func (p *statsProcessor) sendLogStatsFor(cid string, statsList []*chqpb.EventStats) {
	p.tenantLock.Lock()
	tenant, found := p.tenants[cid]
	p.tenantLock.Unlock()
	if !found {
		return
	}

	for _, stat := range statsList {
		key := p.toExemplarKey(stat.ServiceName, stat.Fingerprint)
		exemplarBytes, found := tenant.logExemplars.Get(key)
		if !found {
			continue
		}
		stat.Exemplar = exemplarBytes.([]byte)
	}

	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Stats:       statsList}

	if err := p.postLogStats(context.Background(), wrapper); err != nil {
		p.logger.Error("Failed to send log stats", zap.Error(err))
	}
}

func (p *statsProcessor) postLogStats(ctx context.Context, wrapper *chqpb.EventStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(p.statsBatchSize, int64(len(b)))
	endpoint := p.config.Endpoint + "/api/v1/logstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			p.logger.Error("Failed to close response body", zap.Error(err))
		}
	}(resp.Body)
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send log stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

//
// Spans / Traces
//

func (p *statsProcessor) sendSpanStats(organizationID string) func(stats []*chqpb.EventStats) {
	return func(stats []*chqpb.EventStats) {
		p.sendSpanStatsFor(organizationID, stats)
	}
}

func (p *statsProcessor) sendSpanStatsFor(cid string, statsList []*chqpb.EventStats) {
	p.tenantLock.Lock()
	tenant, found := p.tenants[cid]
	p.tenantLock.Unlock()
	if !found {
		return
	}

	for _, stat := range statsList {
		key := p.toExemplarKey(stat.ServiceName, stat.Fingerprint)
		exemplarBytes, found := tenant.traceExemplars.Get(key)
		if !found {
			continue
		}
		stat.Exemplar = exemplarBytes.([]byte)
	}

	wrapper := &chqpb.EventStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Stats:       statsList}

	if err := p.postSpanStats(context.Background(), wrapper); err != nil {
		p.logger.Error("Failed to send span stats", zap.Error(err))
	}
}

func (p *statsProcessor) postSpanStats(ctx context.Context, wrapper *chqpb.EventStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(p.statsBatchSize, int64(len(b)))
	endpoint := p.config.Endpoint + "/api/v1/spanstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send span stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

//
// Metrics
//

func (p *statsProcessor) sendMetricStats(organizationID string) func(stats []*chqpb.MetricStatsWrapper) {
	return func(stats []*chqpb.MetricStatsWrapper) {
		p.sendMetricStatsFor(organizationID, stats)
	}
}

func (p *statsProcessor) sendMetricStatsFor(organizationId string, wrappers []*chqpb.MetricStatsWrapper) {
	statsList := make([]*chqpb.MetricStats, 0)
	exemplarMap := make(map[int64]*chqpb.MetricExemplar)
	for _, wrapper := range wrappers {
		p.tenantLock.Lock()
		tenant, found := p.tenants[organizationId]
		p.tenantLock.Unlock()

		if found {
			fingerprint := p.toMetricExemplarFingerprint(wrapper.Stats.ServiceName, wrapper.Stats.MetricName, wrapper.Stats.MetricType)
			exemplar, exemplarFound := tenant.metricExemplars.Get(fingerprint)
			if exemplarFound {
				exemplarBytes := exemplar.([]byte)
				if _, ok := exemplarMap[fingerprint]; !ok {
					exemplarMap[fingerprint] = &chqpb.MetricExemplar{
						ServiceName: wrapper.Stats.ServiceName,
						MetricName:  wrapper.Stats.MetricName,
						MetricType:  wrapper.Stats.MetricType,
						ProcessorId: p.id.Name(),
						Exemplar:    exemplarBytes,
						CustomerId:  wrapper.Stats.CustomerId,
						CollectorId: wrapper.Stats.CollectorId,
					}
				}
			}
		}
		if wrapper.Dirty {
			slice, err := wrapper.Hll.ToCompactSlice()
			if err != nil {
				continue
			}
			estimate, err := wrapper.GetEstimate()
			if err != nil {
				continue
			}
			wrapper.Stats.CardinalityEstimate = estimate
			wrapper.Stats.Hll = slice
			statsList = append(statsList, wrapper.Stats)
		}
	}

	exemplarList := make([]*chqpb.MetricExemplar, 0)
	for _, exemplar := range exemplarMap {
		exemplarList = append(exemplarList, exemplar)
	}
	wrapper := &chqpb.MetricStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Stats:       statsList,
		Exemplars:   exemplarList,
	}
	if len(statsList) > 0 || len(exemplarMap) > 0 {
		err := p.sendReport(context.Background(), wrapper)
		if err != nil {
			p.logger.Error("Failed to send metric stats", zap.Error(err))
		}
	}
}

func (p *statsProcessor) sendReport(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(p.statsBatchSize, int64(len(b)))
	endpoint := p.config.Endpoint + "/api/v1/metricstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send metric stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
