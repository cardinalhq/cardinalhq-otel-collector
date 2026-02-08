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
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/signalnames"
)

var (
	logsMarshaler    = &plog.JSONMarshaler{}
	metricsMarshaler = &pmetric.JSONMarshaler{}
	tracesMarshaler  = &ptrace.JSONMarshaler{}
)

type supportedSignals interface {
	plog.Logs | pmetric.Metrics | ptrace.Traces
}

type sendWrapper[T supportedSignals] struct {
	Attributes  map[string]string
	PartitionId int64
	Value       T
}

func sendExemplars[T supportedSignals](p *exemplarProcessor, cid, processorId string, batchSize int) func([]*Entry[T]) {
	return func(entries []*Entry[T]) {
		var batch []*sendWrapper[T]

		for _, entry := range entries {
			exemplar := &sendWrapper[T]{
				Attributes:  entry.toAttributes(),
				PartitionId: entry.key,
				Value:       entry.value,
			}
			batch = append(batch, exemplar)

			if len(batch) >= batchSize {
				sendBatchAsync(p, cid, p.ttype, processorId, batch)
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			sendBatchAsync(p, cid, p.ttype, processorId, batch)
		}
	}
}

// marshalTelemetry dispatches to the correct marshaller
func marshalTelemetry[T supportedSignals](t T) (string, error) {
	var b []byte
	var err error
	switch v := any(t).(type) {
	case plog.Logs:
		b, err = logsMarshaler.MarshalLogs(v)
	case pmetric.Metrics:
		b, err = metricsMarshaler.MarshalMetrics(v)
	case ptrace.Traces:
		b, err = tracesMarshaler.MarshalTraces(v)
	default:
		err = errors.New("unsupported telemetry type")
	}
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func sendBatchAsync[T supportedSignals](p *exemplarProcessor, cid string, telemetryType signalnames.Name, processorId string, batch []*sendWrapper[T]) {
	if len(batch) == 0 {
		return
	}

	go func() {
		pbBatch := make([]*chqpb.Exemplar, 0, len(batch))
		for _, wrapper := range batch {
			data, err := marshalTelemetry(wrapper.Value)
			if err != nil {
				p.logger.Error("Failed to marshal telemetry data", zap.Error(err))
				continue
			}
			pbBatch = append(pbBatch, &chqpb.Exemplar{
				Attributes:  wrapper.Attributes,
				PartitionId: wrapper.PartitionId,
				Payload:     data,
			})
		}

		exemplarReport := &chqpb.ExemplarPublishReport{
			OrganizationId: cid,
			TelemetryType:  telemetryType.String(),
			ProcessorId:    processorId,
			Exemplars:      pbBatch,
		}

		err := p.postBatch(context.Background(), telemetryType, exemplarReport)
		if err != nil {
			p.logger.Error("Failed to send exemplars", zap.Error(err))
		}
	}()
}

func (p *exemplarProcessor) postBatch(ctx context.Context, telemetryType signalnames.Name, report *chqpb.ExemplarPublishReport) error {
	endpoint := fmt.Sprintf("%s/api/v1/exemplars/%s", p.config.Endpoint, telemetryType)

	p.logger.Info("Sending exemplars (proto)", zap.String("endpoint", endpoint), zap.String("organization_id", report.OrganizationId), zap.Int("exemplar_count", len(report.Exemplars)))

	marshalled, err := proto.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(marshalled))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send exemplars", zap.Int("status", resp.StatusCode), zap.String("body", string(body)), zap.String("endpoint", endpoint))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
