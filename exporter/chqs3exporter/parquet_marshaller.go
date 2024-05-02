// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type parquetMarshaller struct{}

func (*parquetMarshaller) format() string {
	return "parquet"
}

func newParquetMarshaller() *parquetMarshaller {
	return &parquetMarshaller{}
}

func (s *parquetMarshaller) MarshalLogs(ld plog.Logs) ([]byte, error) {
	return []byte{}, nil
}

func (s *parquetMarshaller) MarshalTraces(_ ptrace.Traces) ([]byte, error) {
	return []byte{}, nil
}

func (s *parquetMarshaller) MarshalMetrics(_ pmetric.Metrics) ([]byte, error) {
	return []byte{}, nil
}
