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
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/tagwriter"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/boxer"
)

type s3Exporter struct {
	id              component.ID
	config          *Config
	logger          *zap.Logger
	tb              table.Translator
	boxer           *boxer.Boxer
	telemetryType   string
	metadata        map[string]string
	telemetry       *exporterTelemetry
	writerCloseFunc context.CancelFunc
	writerClosed    chan struct{}
	taglock         sync.Mutex
	tags            map[string]map[int64]map[string]any
	idsFromEnv      bool
}

const (
	metricFilePrefix = "metrics"
	logFilePrefix    = "logs"
	tracesFilePrefix = "traces"

	parquetFormat = "parquet"
)

func newS3Exporter(config *Config, params exporter.Settings, ttype string) (*s3Exporter, error) {
	metadata := map[string]string{}
	hn, err := os.Hostname()
	if err == nil {
		metadata["cardinalhq-hostname"] = hn
	}
	metadata["cardinalhq-exporter"] = params.ID.String()

	exporterTelemetry, err := newTelemetry(params, ttype)
	if err != nil {
		return nil, err
	}

	s3LogsExporter := &s3Exporter{
		id:            params.ID,
		config:        config,
		logger:        params.Logger,
		tb:            table.NewTableTranslator(),
		metadata:      metadata,
		telemetry:     exporterTelemetry,
		telemetryType: ttype,
		tags:          map[string]map[int64]map[string]any{},
	}
	return s3LogsExporter, nil
}

func (e *s3Exporter) Start(_ context.Context, _ component.Host) error {
	var err error

	filepath := e.config.Buffering.Directory
	if e.config.Buffering.Type == bufferTypeMemory {
		filepath = ""
	}

	e.idsFromEnv = e.config.IDSource == "env"

	opts := []boxer.BoxerOptions{}
	switch e.telemetryType {
	case logFilePrefix:
		opts = append(opts, boxer.WithInterval(e.config.Timeboxes.Logs.Interval))
		opts = append(opts, boxer.WithGrace(e.config.Timeboxes.Logs.GracePeriod))
		opts = append(opts, boxer.WithIntervalCount(e.config.Timeboxes.Logs.OpenIntervalCount))
	case metricFilePrefix:
		opts = append(opts, boxer.WithInterval(e.config.Timeboxes.Metrics.Interval))
		opts = append(opts, boxer.WithGrace(e.config.Timeboxes.Metrics.GracePeriod))
		opts = append(opts, boxer.WithIntervalCount(e.config.Timeboxes.Metrics.OpenIntervalCount))
	case tracesFilePrefix:
		opts = append(opts, boxer.WithInterval(e.config.Timeboxes.Traces.Interval))
		opts = append(opts, boxer.WithGrace(e.config.Timeboxes.Traces.GracePeriod))
		opts = append(opts, boxer.WithIntervalCount(e.config.Timeboxes.Traces.OpenIntervalCount))
	}

	box, err := boxer.BoxerFor(filepath, component.KindExporter, e.id, e.telemetryType, opts...)
	if err != nil {
		return err
	}
	e.boxer = box

	e.writerClosed = make(chan struct{})
	dbtaskContext, doneFunc := context.WithCancel(context.Background())
	e.writerCloseFunc = doneFunc
	go e.databaseTask(dbtaskContext, e.writerClosed)
	return nil
}

func (e *s3Exporter) Shutdown(_ context.Context) error {
	var errs error

	// signal the watcher to stop processing data.  We will flush the
	// remaining data.
	e.logger.Info("Stopping database task.")
	e.writerCloseFunc()
	<-e.writerClosed
	e.logger.Info("database task stopped.")

	allIntervals, err := e.boxer.GetAllIntervals()
	if err != nil {
		errs = multierr.Append(errs, err)
	} else {
		e.logger.Debug("Processing remaining intervals", zap.Int("count", len(allIntervals)), zap.Int64s("intervals", allIntervals))
		for _, interval := range allIntervals {
			if err := e.processInterval(interval); err != nil {
				errs = multierr.Append(errs, err)
			}
		}
	}

	return errs
}

func (e *s3Exporter) databaseTask(ctx context.Context, closedChan chan struct{}) {
	closeTicker := time.NewTicker(1 * time.Second)
	defer closeTicker.Stop()
	defer close(closedChan)
	e.logger.Info("Database task started")
	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Database task exiting")
			return
		case now := <-closeTicker.C:
			e.processClosedTimer(now)
		}
	}
}

func (e *s3Exporter) processClosedTimer(now time.Time) {
	defer func() {
		if err := recover(); err != nil {
			e.logger.Error("Panic in processClosedTimer", zap.Any("error", err))
		}
	}()

	intervals, err := e.boxer.GetClosedIntervals(now)
	if err != nil {
		e.logger.Error("Failed to get closed intervals", zap.Error(err))
		return
	}
	for _, interval := range intervals {
		if err := e.processInterval(interval); err != nil {
			e.logger.Error("Failed to process interval", zap.Error(err))
		}
	}
}

func (e *s3Exporter) processInterval(interval int64) error {
	defer func() {
		if err := e.boxer.CloseInterval(interval); err != nil {
			e.logger.Error("Failed to close interval", zap.Error(err))
		}
	}()

	if err := e.writeInterval(interval); err != nil {
		return err
	}
	return nil
}

func (e *s3Exporter) consumeTags(ids string, interval int64) map[string]any {
	e.taglock.Lock()
	defer e.taglock.Unlock()
	tags, ok := e.tags[ids]
	if !ok {
		return nil
	}
	intervalTags, ok := tags[interval]
	if !ok {
		return nil
	}
	delete(e.tags[ids], interval)
	return intervalTags
}

func (e *s3Exporter) newParquetWriter(ids string, interval int64) (tagwriter.MapWriter, *os.File, error) {
	tags := e.consumeTags(ids, interval)
	if len(tags) == 0 {
		keys := map[string][]int64{}
		for k, v := range e.tags {
			keys[k] = maps.Keys(v)
		}
		customerID, clusterID := splitCustomerID(ids)
		e.logger.Warn("No tags found", zap.String("customerID", customerID), zap.String("clusterID", clusterID), zap.Int64("interval", interval), zap.Any("keys", keys))
		return nil, nil, errors.New("no tags found")
	}

	schema, err := tagwriter.ParquetSchemaFromMap("schema", tags)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parquet schema: %w", err)
	}

	f, err := os.CreateTemp(e.config.Buffering.Directory, "parquet-*")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	writer, err := tagwriter.NewParquetMapWriter(f, schema)
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	return writer, f, nil
}

func ensureCustomerID(tableRows []map[string]any, customerID string, logger *zap.Logger) bool {
	for _, row := range tableRows {
		cid := customerIDFromMap(row)
		if cid != customerID {
			logger.Warn("Customer ID mismatch", zap.String("tableCustomerID", cid))
			return false
		}
	}
	return true
}

func (e *s3Exporter) saveAndUploadParquet(ids string, interval int64) error {
	writer, f, err := e.newParquetWriter(ids, interval)
	if err != nil {
		return err
	}
	customerID, clusterID := splitCustomerID(ids)
	logger := e.logger.With(zap.String("customerID", customerID), zap.String("clusterID", clusterID), zap.Int64("interval", interval), zap.String("tempFilename", f.Name()))
	defer func() {
		if writer != nil {
			if err := writer.Close(); err != nil {
				logger.Error("Failed to close writer", zap.Error(err))
			}
		}
		if f != nil {
			if err := f.Close(); err != nil {
				logger.Error("Failed to close file", zap.Error(err))
			}
			if err := os.Remove(f.Name()); err != nil {
				logger.Error("Failed to remove file", zap.Error(err))
			}
		}
	}()

	blockCount := int64(0)
	itemCount := int64(0)
	err = e.boxer.ForEach(interval, ids, func(value []byte) (bool, error) {
		blockCount++
		logger.Debug("Processing interval")
		tableRows := []map[string]any{}
		if err := gobDecode(value, &tableRows); err != nil {
			logger.Error("Failed to unmarshal table", zap.Error(err))
			return false, err
		}
		if !ensureCustomerID(tableRows, customerID, logger) {
			return false, fmt.Errorf("customer ID mismatch")
		}
		logger.Debug("Writing rows", zap.Int("count", len(tableRows)))
		if _, err := writer.WriteRows(tableRows); err != nil {
			logger.Error("Failed to write rows", zap.Error(err))
			return false, err
		}
		itemCount += int64(len(tableRows))
		return true, nil
	})
	if err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}
	writer = nil

	if size, err := filesize(f); err != nil {
		logger.Error("Failed to get file size, assuming something useful is there...", zap.Error(err))
	} else if size == 0 {
		logger.Info("Skipping empty file")
		return nil
	}

	e.telemetry.blocksReadTemp.Add(context.Background(), blockCount, metric.WithAttributeSet(e.telemetry.aset))
	e.telemetry.itemsReadTemp.Add(context.Background(), itemCount, metric.WithAttributeSet(e.telemetry.aset))

	return e.upload(f, &s3Writer{}, ids, interval)
}

func (e *s3Exporter) writeInterval(interval int64) error {
	ids, err := e.boxer.GetScopesForInterval(interval)
	if err != nil {
		return err
	}

	for _, id := range ids {
		defer func(id string) {
			if err := e.boxer.CloseIntervalScope(interval, id); err != nil {
				e.logger.Error("Failed to close interval scope", zap.Error(err))
			}
		}(id)
		if err := e.saveAndUploadParquet(id, interval); err != nil {
			e.logger.Error("Failed to save and upload parquet", zap.Error(err))
			return err
		}
	}

	return nil
}

func filesize(f *os.File) (int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (e *s3Exporter) upload(f io.ReadSeeker, writer filewriter, ids string, interval int64) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of file: %w", err)
	}
	now := e.boxer.TimeForInterval(interval)
	prefix := e.telemetryType + "_" + strconv.FormatInt(now.UnixMilli(), 10)
	customerID, clusterID := splitCustomerID(ids)
	e.logger.Info("Uploading file", zap.String("customerID", customerID), zap.String("clusterID", clusterID), zap.String("prefix", prefix))
	return writer.writeBuffer(context.Background(), now, f, e.config, prefix, parquetFormat, e.metadata, ids)
}
