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

func (e *s3Exporter) writeClosed(customerIDs []string, oldestTimestamp int64, filePrefix string, acc timebox.Timebox[string, *TimeboxEntry]) {
	e.telemetry.startGoProcWriter()
	defer e.telemetry.finishGoProcWriter()
	for _, customerID := range customerIDs {
		items, err := acc.Closed(customerID, oldestTimestamp, &TimeboxEntry{})
		if err != nil {
			e.logger.Error("Failed to get closed tables", zap.Error(err), zap.String("filePrefix", filePrefix), zap.Int64("oldestTimestamp", oldestTimestamp), zap.Strings("customerIDs", customerIDs))
			continue
		}
		if err := e.writeTable(items, filePrefix, customerID); err != nil {
			e.logger.Error("Failed to write closed tables", zap.Error(err), zap.String("filePrefix", filePrefix), zap.Int64("oldestTimestamp", oldestTimestamp), zap.Strings("customerIDs", customerIDs))
		}
	}
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
		starttime := time.Now()
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
		processingDuration := time.Since(starttime)
		logger.Info("Wrote buffer", zap.String("prefix", prefix), zap.Int("rows", len(rows)), zap.Duration("duration", processingDuration))
		s.telemetry.filesWritten.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("telemetryType", telemetryType), attribute.Bool("success", true)))
	}

	return nil
}

func (e *s3Exporter) Shutdown(context.Context) error {
	var errs error

	scopes := e.logs.Scopes()
	for _, scope := range scopes {
		items, err := e.logs.Closed(scope, 0, &TimeboxEntry{})
		if err != nil {
			errs = multierr.Append(errs, err)
		}
		errs = multierr.Append(errs, e.writeTable(items, logFilePrefix, scope))
	}

	scopes = e.metrics.Scopes()
	for _, scope := range scopes {
		items, err := e.metrics.Closed(scope, 0, &TimeboxEntry{})
		if err != nil {
			errs = multierr.Append(errs, err)
		}
		errs = multierr.Append(errs, e.writeTable(items, metricFilePrefix, scope))
	}

	scopes = e.traces.Scopes()
	for _, scope := range scopes {
		items, err := e.traces.Closed(scope, 0, &TimeboxEntry{})
		if err != nil {
			errs = multierr.Append(errs, err)
		}
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
