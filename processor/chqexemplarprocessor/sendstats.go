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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type Exemplar struct {
	Payload    []byte            `json:"payload"`
	Attributes map[string]string `json:"attributes"`
}
type ExemplarPublishReport struct {
	CustomerId    string      `json:"customer_id"`
	ProcessorId   string      `json:"processor_id"`
	TelemetryType string      `json:"telemetry_type"`
	Exemplars     []*Exemplar `json:"exemplars"`
}

func (p *exemplarProcessor) sendExemplars(cid, telemetryType, processorId string) func([]*Entry) {
	return func(entries []*Entry) {
		var batch []*Exemplar
		accumulated := 0
		batchSize := 50

		for _, entry := range entries {
			var data []byte
			var err error

			switch telemetryType {
			case "logs":
				data, err = p.jsonMarshaller.logsMarshaler.MarshalLogs(entry.value.(plog.Logs))
			case "metrics":
				data, err = p.jsonMarshaller.metricsMarshaler.MarshalMetrics(entry.value.(pmetric.Metrics))
			case "traces":
				data, err = p.jsonMarshaller.tracesMarshaler.MarshalTraces(entry.value.(ptrace.Traces))
			default:
				p.logger.Error("Unknown telemetry type", zap.String("type", telemetryType))
				continue
			}

			if err != nil {
				p.logger.Error("Failed to marshal telemetry data", zap.Error(err))
				continue
			}
			exemplar := &Exemplar{
				Payload:    data,
				Attributes: entry.toAttributes(),
			}
			batch = append(batch, exemplar)
			accumulated++

			if accumulated >= batchSize {
				p.sendBatchAsync(cid, telemetryType, processorId, batch)
				accumulated = 0
				batch = nil
			}
		}

		// Send remaining batch if any
		if accumulated > 0 {
			p.sendBatchAsync(cid, telemetryType, processorId, batch)
		}
	}
}

func (p *exemplarProcessor) sendBatchAsync(cid, telemetryType, processorId string, batch []*Exemplar) {
	if len(batch) == 0 {
		return
	}

	exemplarReport := &ExemplarPublishReport{
		CustomerId:    cid,
		TelemetryType: telemetryType,
		ProcessorId:   processorId,
		Exemplars:     batch,
	}

	go func() {
		err := p.postBatch(context.Background(), telemetryType, exemplarReport)
		if err != nil {
			p.logger.Error("Failed to send exemplars", zap.Error(err))
		}
	}()
}

func (p *exemplarProcessor) postBatch(ctx context.Context, telemetryType string, report *ExemplarPublishReport) error {
	endpoint := fmt.Sprintf("%s/api/v1/exemplars/%s", p.config.Endpoint, telemetryType)

	// Marshal report to JSON
	marshalled, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %w", err)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(marshalled))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set proper JSON header
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send exemplars", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
