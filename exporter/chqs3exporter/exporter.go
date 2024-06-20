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
	"golang.org/x/exp/maps"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/tagwriter"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/timebox"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
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

	var bufferFactory timebox.BufferFactory
	if config.Buffering.Type == bufferTypeDisk {
		params.Logger.Info("Using disk buffer")
		bufferFactory = timebox.NewBufferFilesystemFactory(config.Buffering.Directory)
	} else {
		params.Logger.Info("Using memory buffer")
		bufferFactory = timebox.NewMemoryBufferFactory()
	}

	s3Exporter := &s3Exporter{
		config:     config,
		dataWriter: &s3Writer{},
		logger:     params.Logger,
		tb:         table.NewTableTranslator(),
		logs: timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory,
			config.Timeboxes.Logs.Interval,
			config.Timeboxes.Logs.OpenIntervalCount,
			config.Timeboxes.Logs.GracePeriod),
		metrics: timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory,
			config.Timeboxes.Metrics.Interval,
			config.Timeboxes.Metrics.OpenIntervalCount,
			config.Timeboxes.Metrics.GracePeriod),
		traces: timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory,
			config.Timeboxes.Traces.Interval,
			config.Timeboxes.Traces.OpenIntervalCount,
			config.Timeboxes.Traces.GracePeriod),
		metadata:  metadata,
		telemetry: exporterTelemetry,
	}
	return s3Exporter
}

func (e *s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var errs error
	oldestTimestamp, customerIDs, err := e.consumeMetrics(time.Now().UnixMilli(), md)
	errs = multierr.Append(errs, err)
	for _, customerID := range customerIDs {
		items := e.metrics.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
		errs = multierr.Append(errs, e.writeTable(items, metricFilePrefix, customerID))
	}
	return errs
}

func (e *s3Exporter) consumeMetrics(now int64, md pmetric.Metrics) (int64, []string, error) {
	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return 0, nil, nil
	}
	return e.appendMetrics(now, md)
}

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	var errs error
	oldestTimestamp, customerIDs, err := e.consumeLogs(time.Now().UnixMilli(), logs)
	errs = multierr.Append(errs, err)
	for _, customerID := range customerIDs {
		items := e.logs.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
		errs = multierr.Append(errs, e.writeTable(items, logFilePrefix, customerID))
	}
	return errs
}

func (e *s3Exporter) consumeLogs(now int64, logs plog.Logs) (int64, []string, error) {
	if e.config.Timeboxes.Logs.Interval <= 0 {
		return 0, nil, nil
	}
	return e.appendLogs(now, logs)
}

func (e *s3Exporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	var errs error
	oldestTimestamp, customerIDs, err := e.consumeTraces(time.Now().UnixMilli(), traces)
	errs = multierr.Append(errs, err)
	for _, customerID := range customerIDs {
		items := e.traces.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
		errs = multierr.Append(errs, e.writeTable(items, tracesFilePrefix, customerID))
	}
	return errs
}

func (e *s3Exporter) consumeTraces(now int64, traces ptrace.Traces) (int64, []string, error) {
	if e.config.Timeboxes.Traces.Interval <= 0 {
		return 0, nil, nil
	}
	return e.appendTraces(now, traces)
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

func (e *s3Exporter) appendMetrics(now int64, md pmetric.Metrics) (int64, []string, error) {
	tbl, err := e.tb.MetricsFromOtel(&md)
	if err != nil {
		return 0, nil, err
	}
	return e.emitRows(now, false, tbl, e.metrics, metricFilePrefix)
}

func (e *s3Exporter) appendTraces(now int64, td ptrace.Traces) (int64, []string, error) {
	tbl, err := e.tb.TracesFromOtel(&td)
	if err != nil {
		return 0, nil, err
	}
	return e.emitRows(now, true, tbl, e.traces, tracesFilePrefix)
}

func (e *s3Exporter) appendLogs(now int64, ld plog.Logs) (int64, []string, error) {
	tbl, err := e.tb.LogsFromOtel(&ld)
	if err != nil {
		return 0, nil, err
	}
	return e.emitRows(now, true, tbl, e.logs, logFilePrefix)
}

func customerIDFromMap(m map[string]any) string {
	customerID, found := m[translate.CardinalFieldCustomerID]
	if !found {
		return ""
	}
	if cid, ok := customerID.(string); ok {
		return cid
	}
	return ""
}

func (e *s3Exporter) emitRows(now int64, useWallclock bool, tbl []map[string]interface{}, tbox timebox.Timebox[string, *TimeboxEntry], filePrefix string) (oldest int64, customerIDs []string, err error) {
	cids := map[string]any{}
	for _, row := range tbl {
		tbe := TimeboxEntry(row)
		customerID := customerIDFromMap(row)
		cids[customerID] = nil
		ts := e.emitInto(now, useWallclock, tbox, &tbe, filePrefix, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, maps.Keys(cids), nil
}

func (e *s3Exporter) emitInto(now int64, useWallclock bool, acc timebox.Timebox[string, *TimeboxEntry], item *TimeboxEntry, telemetryType, customerID string) int64 {
	itemts := now
	if !useWallclock {
		itemts = item.ItemTS()
	}
	if old, oldIntervals := acc.TooOld(itemts, now); old {
		if oldIntervals > 10 {
			oldIntervals = 99
		}
		e.telemetry.datapointTooOld.Add(context.Background(), 1, metric.WithAttributes(
			attribute.String("telemetryType", telemetryType),
			attribute.String("customerID", customerID),
			attribute.Int64("ageInIntervals", oldIntervals)))
		return 0
	}
	err := acc.Append(customerID, itemts, item)
	if err != nil {
		e.logger.Error("Failed to append item", zap.Error(err))
	}
	return itemts
}
