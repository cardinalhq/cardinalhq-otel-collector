// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
)

type parquetMarshaller struct {
	tb table.Translator

	logs    map[int64][]map[string]any
	traces  map[int64][]map[string]any
	metrics map[int64][]map[string]any
}

func (*parquetMarshaller) format() string {
	return "parquet"
}

func newParquetMarshaller() *parquetMarshaller {
	return &parquetMarshaller{
		tb: table.NewTableTranslator(),

		logs:    map[int64][]map[string]any{},
		traces:  map[int64][]map[string]any{},
		metrics: map[int64][]map[string]any{},
	}
}

func (s *parquetMarshaller) MarshalLogs(tb int64) ([]byte, error) {
	return []byte{}, nil
}

func (s *parquetMarshaller) MarshalTraces(tb int64) ([]byte, error) {
	return []byte{}, nil
}

func (s *parquetMarshaller) MarshalMetrics(tb int64) ([]byte, error) {
	return []byte{}, nil
}

func (s *parquetMarshaller) appendMetrics(md pmetric.Metrics) []int64 {
	tbl := []map[string]any{}

	return nil
}

func (s *parquetMarshaller) appendTraces(td ptrace.Traces) []int64 {
	return nil
}

func (s *parquetMarshaller) appendLogs(ld plog.Logs) []int64 {
	return nil
}

func (s *parquetMarshaller) tableForMetrics(md pmetric.Metrics) (map[string]any, error) {
	return s.tb.MetricsFromOtel(&md)
}
