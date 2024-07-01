// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/sdk/metric"
)

// nolint: unused
var testLogs = []byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"_sourceCategory","value":{"stringValue":"logfile"}},{"key":"_sourceHost","value":{"stringValue":"host"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"observedTimeUnixNano":"1654257420681895000","body":{"stringValue":"2022-06-03 13:57:00.62739 +0200 CEST m=+14.018296742 log entry14"},"attributes":[{"key":"log.file.path_resolved","value":{"stringValue":"logwriter/data.log"}}],"traceId":"","spanId":""}]}],"schemaUrl":"https://opentelemetry.io/schemas/1.6.1"}]}`)

// nolint: unused
type TestWriter struct {
	t *testing.T
}

// nolint: unused
func (testWriter *TestWriter) writeBuffer(_ context.Context, _ time.Time, buf io.Reader, _ *Config, _ string, _ string, _ map[string]string, _ string) error {
	b, err := io.ReadAll(buf)
	assert.NoError(testWriter.t, err)
	assert.NotZero(testWriter.t, len(b))
	assert.Equal(testWriter.t, []byte{'P', 'A', 'R', '1'}, b[:4])
	return nil
}

// nolint: unused
func getTestLogs(tb testing.TB) plog.Logs {
	logsMarshaler := plog.JSONUnmarshaler{}
	logs, err := logsMarshaler.UnmarshalLogs(testLogs)
	assert.NoError(tb, err, "Can't unmarshal testing logs data -> %s", err)
	assert.Equal(tb, logs.ResourceLogs().Len(), 1)
	return logs
}

// nolint: unused
func dummyTelemetry() *exporterTelemetry {
	meter := metric.NewMeterProvider()
	m, _ := meter.Meter("test").Int64Counter("test")
	return &exporterTelemetry{
		filesWritten:    m,
		datapointTooOld: m,
	}
}
