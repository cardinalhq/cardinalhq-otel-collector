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
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.uber.org/multierr"
	"go.uber.org/zap"

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
	s3              *s3Writer
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

	exporterTelemetry, err := newTelemetry(params)
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
		s3:            &s3Writer{},
	}
	return s3LogsExporter, nil
}

func (e *s3Exporter) Start(_ context.Context, _ component.Host) error {
	var err error

	filepath := e.config.Buffering.Directory
	if e.config.Buffering.Type == bufferTypeMemory {
		filepath = ""
	}
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
	_ = e.boxer.Wipe()

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
		e.logger.Info("Processing remaining intervals", zap.Int("count", len(allIntervals)), zap.Int64s("intervals", allIntervals))
		for _, interval := range allIntervals {
			if err := e.processInterval(interval); err != nil {
				errs = multierr.Append(errs, err)
			}
		}
	}

	return errs
}

func (e *s3Exporter) databaseTask(ctx context.Context, closedChan chan struct{}) {
	maintainTicker := time.NewTicker(5 * time.Minute)
	closeTicker := time.NewTicker(1 * time.Second)
	defer maintainTicker.Stop()
	defer closeTicker.Stop()
	defer close(closedChan)
	e.logger.Info("Database task started")
	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Database task exiting")
			return
		case <-maintainTicker.C:
			e.processMaintainTimer()
		case now := <-closeTicker.C:
			e.processClosedTimer(now)
		}
	}
}

func (e *s3Exporter) processMaintainTimer() {
	defer func() {
		if err := recover(); err != nil {
			e.logger.Error("Panic in processMaintainTimer", zap.Any("error", err))
		}
	}()

	e.logger.Info("Maintaining database")
	if err := e.boxer.Maintain(); err != nil {
		e.logger.Error("Failed to maintain database", zap.Error(err))
	}
	e.logger.Info("Database maintained")
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
	allIntervals, err := e.boxer.GetAllIntervals()
	if err != nil {
		e.logger.Error("Failed to get all intervals", zap.Error(err))
	}
	e.logger.Info("Checking for closed intervals",
		zap.Int64s("intervals", intervals),
		zap.Int64("current_interval", e.boxer.IntervalForTime(now)),
		zap.Int64s("all_intervals", allIntervals))
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

func (e *s3Exporter) writeInterval(interval int64) error {
	lastCustomerID := ""
	var tw *tagwriter.ParquetMapWriter
	var fw *os.File
	defer func() {
		if tw != nil {
			err := tw.Close()
			if err != nil {
				e.logger.Error("Failed to close parquet writer", zap.Error(err))
			}
		}
		if fw != nil {
			if err := os.Remove(fw.Name()); err != nil {
				e.logger.Error("Failed to remove temp file", zap.Error(err))
			}
			err := fw.Close()
			if err != nil {
				e.logger.Error("Failed to close file", zap.Error(err))
			}
		}
	}()

	err := e.boxer.ForEach(interval, func(customerID string, ts time.Time, value []byte) bool {
		if customerID != lastCustomerID {
			if tw != nil {
				if err := tw.Close(); err != nil {
					e.logger.Error("Failed to close parquet writer", zap.Error(err))
				}
				tw = nil
				err := e.upload(fw, customerID, interval)
				if err != nil {
					e.logger.Error("Failed to upload file", zap.Error(err))
					return false
				}
				if err := os.Remove(fw.Name()); err != nil {
					e.logger.Error("Failed to remove temp file", zap.Error(err))
				}
				if err := fw.Close(); err != nil {
					e.logger.Error("Failed to close file", zap.Error(err))
				}
				fw = nil
			}
			lastCustomerID = customerID
			if tw == nil {
				e.taglock.Lock()
				tags := e.tags[customerID][interval]
				delete(e.tags[customerID], interval)
				e.taglock.Unlock()
				typemap := map[string]string{}
				for k, v := range tags {
					typemap[k] = fmt.Sprintf("%T", v)
				}
				e.logger.Info("Creating parquet writer", zap.String("customerID", customerID), zap.Int64("interval", interval), zap.Any("typemap", typemap))
				schema, err := tagwriter.ParquetSchemaFromMap("schema", tags)
				if err != nil {
					e.logger.Error("Failed to create parquet schema", zap.Error(err))
					return false
				}
				fw, err := os.CreateTemp(e.config.Buffering.Directory, "parquet-*")
				if err != nil {
					e.logger.Error("Failed to create temp file", zap.Error(err))
					return false
				}
				tw, err = tagwriter.NewParquetMapWriter(fw, schema)
				if err != nil {
					e.logger.Error("Failed to create parquet writer", zap.Error(err))
					return false
				}
			}
		}
		tableRows := []map[string]any{}
		if err := gobDecode(value, &tableRows); err != nil {
			e.logger.Error("Failed to unmarshal table", zap.Error(err))
			return true
		}
		if _, err := tw.WriteRows(tableRows); err != nil {
			e.logger.Error("Failed to write row", zap.Error(err))
			return true
		}
		return true
	})
	if tw != nil {
		err := e.upload(fw, lastCustomerID, interval)
		if err != nil {
			e.logger.Error("Failed to upload file", zap.Error(err))
			return err
		}
	}

	return err
}

func (e *s3Exporter) upload(f *os.File, customerID string, interval int64) error {
	if _, err := f.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to start of file: %w", err)
	}
	prefix := e.telemetryType + "_" + strconv.FormatInt(interval, 10)
	now := e.boxer.TimeForInterval(interval)
	e.logger.Info("Uploading file", zap.String("customerID", customerID), zap.String("prefix", prefix))
	return e.s3.writeBuffer(context.Background(), now, f, e.config, prefix, parquetFormat, e.metadata, customerID)
}
