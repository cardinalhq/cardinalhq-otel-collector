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
	"encoding/json"
	"fmt"
	"io"
	"log"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/tagwriter"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/timebox"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
)

type parquetMarshaller struct {
	tb table.Translator

	logconfig TimeboxConfig
	logs      map[int64][]map[string]any

	metricconfig TimeboxConfig
	metrics      map[int64][]map[string]any

	traceconfig TimeboxConfig
	traces      map[int64][]map[string]any
}

func (*parquetMarshaller) format() string {
	return "parquet"
}

func newParquetMarshaller(tbconf *TimeboxesConfig) *parquetMarshaller {
	return &parquetMarshaller{
		tb: table.NewTableTranslator(),

		logs:      map[int64][]map[string]any{},
		logconfig: tbconf.Logs,

		metrics:      map[int64][]map[string]any{},
		metricconfig: tbconf.Metrics,

		traces:      map[int64][]map[string]any{},
		traceconfig: tbconf.Traces,
	}
}

func (s *parquetMarshaller) MarshalTable(wr io.Writer, items []map[string]any) error {
	return processTable(items, wr)
}

func processTable(items []map[string]any, wr io.Writer) error {
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
	return nil
}

func closed(now, tbstart, interval, grace int64) bool {
	return now-tbstart >= interval+grace
}

func (s *parquetMarshaller) ClosedLogs(t int64) map[int64][]map[string]any {
	return s.closed(t, s.logs)
}

func (s *parquetMarshaller) ClosedMetrics(t int64) map[int64][]map[string]any {
	return s.closed(t, s.metrics)
}

func (s *parquetMarshaller) ClosedTraces(t int64) map[int64][]map[string]any {
	return s.closed(t, s.traces)
}

func (s *parquetMarshaller) closed(t int64, m map[int64][]map[string]any) map[int64][]map[string]any {
	ret := map[int64][]map[string]any{}
	for k, v := range m {
		if t == 0 || closed(t, k, s.logconfig.Interval, s.logconfig.GracePeriod) {
			ret[k] = v
			delete(s.logs, k)
		}
	}
	return ret
}

func (s *parquetMarshaller) appendMetrics(now int64, md pmetric.Metrics) error {
	tbl, err := s.tb.MetricsFromOtel(&md)
	if err != nil {
		return err
	}
	for _, row := range tbl {
		emitInto(s.metrics, now, row)
	}
	return nil
}

func (s *parquetMarshaller) appendTraces(now int64, td ptrace.Traces) error {
	tbl, err := s.tb.TracesFromOtel(&td)
	if err != nil {
		return err
	}
	for _, row := range tbl {
		emitInto(s.traces, now, row)
	}
	return nil
}

func (s *parquetMarshaller) appendLogs(now int64, ld plog.Logs) error {
	tbl, err := s.tb.LogsFromOtel(&ld)
	if err != nil {
		return err
	}
	for _, row := range tbl {
		emitInto(s.logs, now, row)
	}
	dump(s.logs)
	return nil
}

func emitInto(acc map[int64][]map[string]any, interval int64, item map[string]any) {
	itemts, ok := item["timestamp"].(int64)
	if !ok {
		return
	}
	ch := timebox.Timebox(itemts, interval)
	if _, ok := acc[ch]; !ok {
		acc[ch] = []map[string]any{}
	}
	acc[ch] = append(acc[ch], item)
}

func dump(x any) {
	b, _ := json.MarshalIndent(x, "", "  ")
	log.Printf("%s", b)
}
