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
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/tagwriter"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/timebox"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
)

type s3Exporter struct {
	config     *Config
	dataWriter dataWriter
	logger     *zap.Logger
	tb         table.Translator
	logs       timebox.Timebox[string, *TimeboxEntry]
	metrics    timebox.Timebox[string, *TimeboxEntry]
	traces     timebox.Timebox[string, *TimeboxEntry]
	metadata   map[string]string
	telemetry  *exporterTelemetry
}

const (
	metricFilePrefix = "metrics"
	logFilePrefix    = "logs"
	tracesFilePrefix = "traces"

	parquetFormat = "parquet"
)

func newS3Exporter(config *Config, params exporter.Settings) *s3Exporter {
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

	bufferFactory := timebox.NewMemoryBufferFactory()

	s3Exporter := &s3Exporter{
		config:     config,
		dataWriter: &s3Writer{},
		logger:     params.Logger,
		tb:         table.NewTableTranslator(),
		logs:       timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory, config.Timeboxes.Logs.Interval, config.Timeboxes.Logs.OpenIntervalCount, config.Timeboxes.Logs.GracePeriod),
		metrics:    timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory, config.Timeboxes.Metrics.Interval, config.Timeboxes.Logs.OpenIntervalCount, config.Timeboxes.Metrics.GracePeriod),
		traces:     timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory, config.Timeboxes.Traces.Interval, config.Timeboxes.Logs.OpenIntervalCount, config.Timeboxes.Traces.GracePeriod),
		metadata:   metadata,
		telemetry:  exporterTelemetry,
	}
	return s3Exporter
}

func (e *s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func customerIDFromContext(ctx context.Context, metadata Metadata) string {
	if metadata.customerIDSource == customerIDSourceNone {
		return ""
	}

	ci := client.FromContext(ctx)
	switch metadata.customerIDSource {
	case customerIDSourceMetadata:
		mdparts := ci.Metadata.Get(metadata.customerIDKey)
		if len(mdparts) == 0 {
			return ""
		}
		return strings.Join(mdparts, ";")

	case customerIDSourceAuth:
		if ci.Auth == nil {
			return ""
		}
		val := ci.Auth.GetAttribute(metadata.customerIDKey)
		switch val := val.(type) {
		case nil:
			return ""
		case string:
			return val
		case []string:
			return strings.Join(val, ";")
		default:
			return "" // TODO log warning that the field is not usable
		}

	default:
		return ""
	}
}

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var errs error
	customerID := customerIDFromContext(ctx, e.config.Metadata)
	oldestTimestamp, err := e.consumeMetrics(time.Now().UnixMilli(), md, customerID)
	errs = multierr.Append(errs, err)
	items := e.metrics.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
	errs = multierr.Append(errs, e.writeTable(items, logFilePrefix, customerID))
	return errs
}

func (e *s3Exporter) consumeMetrics(now int64, md pmetric.Metrics, customerID string) (int64, error) {
	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return 0, nil
	}
	return e.appendMetrics(now, md, customerID)
}

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	var errs error
	customerID := customerIDFromContext(ctx, e.config.Metadata)
	oldestTimestamp, err := e.consumeLogs(time.Now().UnixMilli(), logs, customerID)
	errs = multierr.Append(errs, err)
	items := e.logs.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
	errs = multierr.Append(errs, e.writeTable(items, logFilePrefix, customerID))
	return errs
}

func (e *s3Exporter) consumeLogs(now int64, logs plog.Logs, customerID string) (int64, error) {
	if e.config.Timeboxes.Logs.Interval <= 0 {
		return 0, nil
	}
	return e.appendLogs(now, logs, customerID)
}

func (e *s3Exporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	var errs error
	customerID := customerIDFromContext(ctx, e.config.Metadata)
	oldestTimestamp, err := e.consumeTraces(time.Now().UnixMilli(), traces, customerID)
	errs = multierr.Append(errs, err)
	items := e.traces.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
	errs = multierr.Append(errs, e.writeTable(items, tracesFilePrefix, customerID))
	return errs
}

func (e *s3Exporter) consumeTraces(now int64, traces ptrace.Traces, customerID string) (int64, error) {
	if e.config.Timeboxes.Traces.Interval <= 0 {
		return 0, nil
	}
	return e.appendTraces(now, traces, customerID)
}

func (s *s3Exporter) writeTable(items map[int64][]*TimeboxEntry, telemetryType string, customerID string) error {
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
		logger := s.logger.With(zap.String("customerID", customerID), zap.String("telemetryType", telemetryType), zap.Int64("timebox", tb))
		err := s.MarshalTable(wr, rows)
		if err != nil {
			logger.Error("Failed to marshal table", zap.Error(err))
			continue
		}
		prefix := telemetryType + "_" + strconv.FormatInt(tb, 10)
		now := time.UnixMilli(tb)
		err = s.dataWriter.writeBuffer(context.Background(), now, wr, s.config, prefix, parquetFormat, s.metadata, customerID)
		if err != nil {
			s.telemetry.filesWritten.Add(context.Background(), 1,
				metric.WithAttributes(attribute.String("telemetryType", telemetryType), attribute.Bool("success", false)))
			logger.Error("Failed to write buffer", zap.Error(err))
			continue
		}
		logger.Info("Wrote buffer", zap.String("prefix", prefix), zap.Int("rows", len(rows)))
		s.telemetry.filesWritten.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("telemetryType", telemetryType), attribute.Bool("success", true)))
	}

	return nil
}

func (e *s3Exporter) Shutdown(context.Context) error {
	var errs error

	scopes := e.logs.Scopes()
	for _, scope := range scopes {
		items := e.logs.Closed(scope, 0, &TimeboxEntry{})
		errs = multierr.Append(errs, e.writeTable(items, logFilePrefix, scope))
	}

	scopes = e.metrics.Scopes()
	for _, scope := range scopes {
		items := e.metrics.Closed(scope, 0, &TimeboxEntry{})
		errs = multierr.Append(errs, e.writeTable(items, metricFilePrefix, scope))
	}

	scopes = e.traces.Scopes()
	for _, scope := range scopes {
		items := e.traces.Closed(scope, 0, &TimeboxEntry{})
		errs = multierr.Append(errs, e.writeTable(items, tracesFilePrefix, scope))
	}
	return errs
}

func (s *s3Exporter) MarshalTable(wr io.Writer, items []*TimeboxEntry) error {
	table := map[string]any{}
	for _, item := range items {
		for k, v := range *item {
			current, ok := table[k]
			if ok {
				if fmt.Sprintf("%T", current) != fmt.Sprintf("%T", v) {
					return fmt.Errorf("Mismatched types: key = %s: %T %T", k, current, v)
				}
			} else {
				table[k] = v
			}
		}
	}
	schema, err := tagwriter.ParquetSchemaFromMap("schema", table)
	if err != nil {
		return err
	}
	writer, err := tagwriter.NewParquetMapWriter(wr, schema)
	if err != nil {
		return err
	}
	mapItems := make([]map[string]any, len(items))
	for i, item := range items {
		mapItems[i] = map[string]any(*item)
	}
	_, err = writer.WriteRows([]map[string]any(mapItems))
	if err != nil {
		return err
	}
	return writer.Close()
}

func (e *s3Exporter) appendMetrics(now int64, md pmetric.Metrics, customerID string) (int64, error) {
	tbl, err := e.tb.MetricsFromOtel(&md)
	if err != nil {
		return 0, err
	}
	oldest := int64(0)
	for _, row := range tbl {
		tbe := TimeboxEntry(row)
		ts := e.emitInto(now, e.metrics, &tbe, metricFilePrefix, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, nil
}

func (e *s3Exporter) appendTraces(now int64, td ptrace.Traces, customerID string) (int64, error) {
	tbl, err := e.tb.TracesFromOtel(&td)
	if err != nil {
		return 0, err
	}
	oldest := int64(0)
	for _, row := range tbl {
		tbe := TimeboxEntry(row)
		ts := e.emitInto(now, e.traces, &tbe, tracesFilePrefix, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, nil
}

func (e *s3Exporter) appendLogs(now int64, ld plog.Logs, customerID string) (int64, error) {
	tbl, err := e.tb.LogsFromOtel(&ld)
	if err != nil {
		return 0, err
	}
	oldest := int64(0)
	for _, row := range tbl {
		tbe := TimeboxEntry(row)
		ts := e.emitInto(now, e.logs, &tbe, logFilePrefix, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, nil
}

func (e *s3Exporter) emitInto(now int64, acc timebox.Timebox[string, *TimeboxEntry], item *TimeboxEntry, telemetryType, customerID string) int64 {
	itemts := item.ItemTS()
	if acc.TooOld(itemts, now) {
		e.telemetry.datapointTooOld.Add(context.Background(), 1, metric.WithAttributes(
			attribute.String("telemetryType", telemetryType),
			attribute.String("customerID", customerID),
			attribute.Int64("itemts", itemts)))
		return 0
	}
	acc.Append(customerID, itemts, item)
	return itemts
}
