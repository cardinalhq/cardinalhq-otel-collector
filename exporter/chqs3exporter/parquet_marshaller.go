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
	"fmt"
	"io"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/tagwriter"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/timebox"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
)

type parquetMarshaller struct {
	tb      table.Translator
	logs    timebox.Timebox[string]
	metrics timebox.Timebox[string]
	traces  timebox.Timebox[string]
}

func (*parquetMarshaller) format() string {
	return "parquet"
}

func newParquetMarshaller(tbconf *TimeboxesConfig) *parquetMarshaller {
	return &parquetMarshaller{
		tb:      table.NewTableTranslator(),
		logs:    timebox.NewTimeboxImpl[string](tbconf.Logs.Interval, tbconf.Logs.GracePeriod),
		metrics: timebox.NewTimeboxImpl[string](tbconf.Metrics.Interval, tbconf.Metrics.GracePeriod),
		traces:  timebox.NewTimeboxImpl[string](tbconf.Traces.Interval, tbconf.Traces.GracePeriod),
	}
}

func (s *parquetMarshaller) MarshalTable(wr io.Writer, items []map[string]any) error {
	table := map[string]any{}
	for _, item := range items {
		for k, v := range item {
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
	_, err = writer.WriteRows(items)
	if err != nil {
		return err
	}
	return writer.Close()
}

func closed(now, tbstart, interval, grace int64) bool {
	return now-tbstart >= interval+grace
}

func (s *parquetMarshaller) ClosedLogs(now int64) map[int64][]map[string]any {
	return s.closed(now, s.logs)
}

func (s *parquetMarshaller) ClosedMetrics(now int64) map[int64][]map[string]any {
	return s.closed(now, s.metrics)
}

func (s *parquetMarshaller) ClosedTraces(now int64) map[int64][]map[string]any {
	return s.closed(now, s.traces)
}

func (s *parquetMarshaller) closed(now int64, m timebox.Timebox[string]) map[int64][]map[string]any {
	ret := map[int64][]map[string]any{}
	forceClose := now == 0
	for tboxInterval, tbox := range m {
		if forceClose || tbox.ShouldClose(now) {
			ret[tboxInterval] = tbox.Items()
			delete(m, tboxInterval)
		}
	}
	return ret
}

func (s *parquetMarshaller) appendMetrics(md pmetric.Metrics, customerID string) (int64, error) {
	tbl, err := s.tb.MetricsFromOtel(&md)
	if err != nil {
		return 0, err
	}
	s.metricsLock.Lock()
	defer s.metricsLock.Unlock()
	oldest := int64(0)
	for _, row := range tbl {
		ts := emitInto(s.metrics, s.metricconfig, row, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, nil
}

func (s *parquetMarshaller) appendTraces(td ptrace.Traces, customerID string) (int64, error) {
	tbl, err := s.tb.TracesFromOtel(&td)
	if err != nil {
		return 0, err
	}
	s.tracesLock.Lock()
	defer s.tracesLock.Unlock()
	oldest := int64(0)
	for _, row := range tbl {
		ts := emitInto(s.traces, s.traceconfig, row, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, nil
}

func (s *parquetMarshaller) appendLogs(ld plog.Logs, customerID string) (int64, error) {
	tbl, err := s.tb.LogsFromOtel(&ld)
	if err != nil {
		return 0, err
	}
	s.logsLock.Lock()
	defer s.logsLock.Unlock()
	oldest := int64(0)
	for _, row := range tbl {
		ts := emitInto(s.logs, s.logconfig, row, customerID)
		if ts > oldest {
			oldest = ts
		}
	}
	return oldest, nil
}

func emitInto(acc timebox.Timebox[string], config TimeboxConfig, item *TimeboxEntry, customerID string) int64 {
	itemts := item.ItemTS()
	acc.Append(customerID, itemts, item)
	return itemts
}
