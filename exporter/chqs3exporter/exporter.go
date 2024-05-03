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
	"bytes"
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type s3Exporter struct {
	config     *Config
	dataWriter dataWriter
	logger     *zap.Logger
	marshaler  *parquetMarshaller
}

func newS3Exporter(config *Config,
	params exporter.CreateSettings) *s3Exporter {

	s3Exporter := &s3Exporter{
		config:     config,
		dataWriter: &s3Writer{},
		logger:     params.Logger,
		marshaler:  newParquetMarshaller(&config.Timeboxes),
	}
	return s3Exporter
}

func (e *s3Exporter) start(_ context.Context, host component.Host) error {
	return nil
}

func (e *s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	now := time.Now().UnixMilli()
	return e.consumeMetrics(now, md)
}

func (e *s3Exporter) consumeMetrics(now int64, md pmetric.Metrics) error {
	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return nil
	}
	return e.marshaler.appendMetrics(now, md)
}

func (e *s3Exporter) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	now := time.Now().UnixMilli()
	return e.consumeLogs(now, logs)
}

func (e *s3Exporter) consumeLogs(now int64, logs plog.Logs) error {
	if e.config.Timeboxes.Logs.Interval <= 0 {
		return nil
	}
	return e.marshaler.appendLogs(now, logs)
}

func (e *s3Exporter) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	now := time.Now().UnixMilli()
	return e.consumeTraces(now, traces)
}

func (e *s3Exporter) consumeTraces(now int64, traces ptrace.Traces) error {
	if e.config.Timeboxes.Traces.Interval <= 0 {
		return nil
	}
	return e.marshaler.appendTraces(now, traces)
}

func (s *s3Exporter) writeTable(items map[int64][]map[string]any, telemetryType string) error {
	if len(items) == 0 {
		return nil
	}
	for tb, rows := range items {
		if len(rows) == 0 {
			continue
		}
		wr := &bytes.Buffer{}
		err := s.marshaler.MarshalTable(wr, rows)
		if err != nil {
			s.logger.Error("Failed to marshal table", zap.Error(err), zap.String("telemetryType", telemetryType), zap.Int64("timebox", tb))
			continue
		}
		prefix := telemetryType + "_" + strconv.FormatInt(tb, 10)
		err = s.dataWriter.writeBuffer(context.Background(), wr, s.config, prefix, s.marshaler.format())
		if err != nil {
			s.logger.Error("Failed to write buffer", zap.Error(err), zap.String("telemetryType", telemetryType), zap.Int64("timebox", tb))
			continue
		}
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
