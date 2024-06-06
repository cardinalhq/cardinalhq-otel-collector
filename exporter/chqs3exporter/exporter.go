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

package chqs3exporter

import (
	"context"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type s3Exporter struct {
	config     *Config
	dataWriter dataWriter
	logger     *zap.Logger
	marshaler  *parquetMarshaller
	metadata   map[string]string
	telemetry  *exporterTelemetry
}

func newS3Exporter(config *Config, params exporter.CreateSettings) *s3Exporter {
	metadata := map[string]string{}
	hn, err := os.Hostname()
	if err == nil {
		metadata["cardinalhq-hostname"] = hn
	}
	metadata["cardinalhq-exporter"] = params.ID.String()

	exporterTelemetry, err := newTelemetry(params)
	if err != nil {
		params.Logger.Error("Failed to create telemetry", zap.Error(err))
		return nil
	}

	s3Exporter := &s3Exporter{
		config:     config,
		dataWriter: &s3Writer{},
		logger:     params.Logger,
		marshaler:  newParquetMarshaller(&config.Timeboxes),
		metadata:   metadata,
		telemetry:  exporterTelemetry,
	}
	return s3Exporter
}

func (e *s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	var errs error
	errs = multierr.Append(errs, e.consumeMetrics(md))
	now := time.Now().UnixMilli()
	items := e.marshaler.ClosedMetrics(now)
	errs = multierr.Append(errs, e.writeTable(items, "metrics"))
	return errs
}

func (e *s3Exporter) consumeMetrics(md pmetric.Metrics) error {
	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return nil
	}
	return e.marshaler.appendMetrics(md)
}

func (e *s3Exporter) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	var errs error
	errs = multierr.Append(errs, e.consumeLogs(logs))
	now := time.Now().UnixMilli()
	items := e.marshaler.ClosedLogs(now)
	errs = multierr.Append(errs, e.writeTable(items, "logs"))
	return errs
}

func (e *s3Exporter) consumeLogs(logs plog.Logs) error {
	if e.config.Timeboxes.Logs.Interval <= 0 {
		return nil
	}
	return e.marshaler.appendLogs(logs)
}

func (e *s3Exporter) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	var errs error
	errs = multierr.Append(errs, e.consumeTraces(traces))
	now := time.Now().UnixMilli()
	items := e.marshaler.ClosedTraces(now)
	errs = multierr.Append(errs, e.writeTable(items, "traces"))
	return errs
}

func (e *s3Exporter) consumeTraces(traces ptrace.Traces) error {
	if e.config.Timeboxes.Traces.Interval <= 0 {
		return nil
	}
	return e.marshaler.appendTraces(traces)
}

func (s *s3Exporter) writeTable(items map[int64][]map[string]any, telemetryType string) error {
	if len(items) == 0 {
		return nil
	}
	wr := getBuffer()
	defer putBuffer(wr)
	for tb, rows := range items {
		if len(rows) == 0 {
			continue
		}
		wr.Reset()
		err := s.marshaler.MarshalTable(wr, rows)
		if err != nil {
			s.logger.Error("Failed to marshal table", zap.Error(err), zap.String("telemetryType", telemetryType), zap.Int64("timebox", tb))
			continue
		}
		prefix := telemetryType + "_" + strconv.FormatInt(tb, 10)
		now := time.UnixMilli(tb)
		err = s.dataWriter.writeBuffer(context.Background(), now, wr, s.config, prefix, s.marshaler.format(), s.metadata)
		if err != nil {
			s.telemetry.filesWritten.Add(context.Background(), 1,
				metric.WithAttributes(attribute.String("telemetryType", telemetryType), attribute.Bool("success", false)))
			s.logger.Error("Failed to write buffer", zap.Error(err), zap.String("telemetryType", telemetryType), zap.Int64("timebox", tb))
			continue
		}
		s.logger.Info("Wrote buffer", zap.String("telemetryType", telemetryType), zap.Int64("timebox", tb), zap.String("prefix", prefix), zap.Int("rows", len(rows)))
		s.telemetry.filesWritten.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("telemetryType", telemetryType), attribute.Bool("success", true)))
	}

	return nil
}

func (e *s3Exporter) Shutdown(context.Context) error {
	var errs error

	logs := e.marshaler.ClosedLogs(0)
	errs = multierr.Append(errs, e.writeTable(logs, "logs"))

	metrics := e.marshaler.ClosedMetrics(0)
	errs = multierr.Append(errs, e.writeTable(metrics, "metrics"))

	traces := e.marshaler.ClosedTraces(0)
	errs = multierr.Append(errs, e.writeTable(traces, "traces"))

	return errs
}
