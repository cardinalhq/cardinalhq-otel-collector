// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type s3Marshaler struct {
	logsMarshaler    plog.Marshaler
	tracesMarshaler  ptrace.Marshaler
	metricsMarshaler pmetric.Marshaler
	logger           *zap.Logger
	compression      string
	fileFormat       string
}

func (marshaler *s3Marshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return marshaler.tracesMarshaler.MarshalTraces(td)
}

func (marshaler *s3Marshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	return marshaler.logsMarshaler.MarshalLogs(ld)
}

func (marshaler *s3Marshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	return marshaler.metricsMarshaler.MarshalMetrics(md)
}

func (marshaler *s3Marshaler) format() string {
	return marshaler.fileFormat
}
