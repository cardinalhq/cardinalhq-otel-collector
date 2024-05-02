// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/chqs3exporter"

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type marshaler interface {
	MarshalTraces(td ptrace.Traces) ([]byte, error)
	MarshalLogs(ld plog.Logs) ([]byte, error)
	MarshalMetrics(md pmetric.Metrics) ([]byte, error)
	format() string
}

func newMarshaler(logger *zap.Logger) (marshaler, error) {
	marshaler := &s3Marshaler{
		logger:           logger,
		fileFormat:       "parquet",
		compression:      "zstd",
		logsMarshaler:    &parquetMarshaller{},
		tracesMarshaler:  &parquetMarshaller{},
		metricsMarshaler: &parquetMarshaller{},
	}
	return marshaler, nil
}
