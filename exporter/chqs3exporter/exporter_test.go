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
	"go.uber.org/zap"
)

var testLogs = []byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"_sourceCategory","value":{"stringValue":"logfile"}},{"key":"_sourceHost","value":{"stringValue":"host"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"observedTimeUnixNano":"1654257420681895000","body":{"stringValue":"2022-06-03 13:57:00.62739 +0200 CEST m=+14.018296742 log entry14"},"attributes":[{"key":"log.file.path_resolved","value":{"stringValue":"logwriter/data.log"}}],"traceId":"","spanId":""}]}],"schemaUrl":"https://opentelemetry.io/schemas/1.6.1"}]}`)

type TestWriter struct {
	t *testing.T
}

func (testWriter *TestWriter) writeBuffer(_ context.Context, now time.Time, buf io.Reader, _ *Config, _ string, _ string, _ map[string]string) error {
	b, err := io.ReadAll(buf)
	assert.NoError(testWriter.t, err)
	assert.NotZero(testWriter.t, len(b))
	assert.Equal(testWriter.t, []byte{'P', 'A', 'R', '1'}, b[:4])
	return nil
}

func getTestLogs(tb testing.TB) plog.Logs {
	logsMarshaler := plog.JSONUnmarshaler{}
	logs, err := logsMarshaler.UnmarshalLogs(testLogs)
	assert.NoError(tb, err, "Can't unmarshal testing logs data -> %s", err)
	assert.Equal(tb, logs.ResourceLogs().Len(), 1)
	return logs
}

func getLogExporter(t *testing.T) *s3Exporter {
	config := createDefaultConfig().(*Config)
	config.Timeboxes.Logs.Interval = 10
	config.Timeboxes.Logs.GracePeriod = 0
	marshaler := newParquetMarshaller(&config.Timeboxes)
	exporter := &s3Exporter{
		config:     config,
		dataWriter: &TestWriter{t},
		logger:     zap.NewNop(),
		marshaler:  marshaler,
	}
	return exporter
}

func TestLog(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporter(t)
	assert.NoError(t, exporter.consumeLogs(logs))
	items := exporter.marshaler.ClosedLogs(0)
	assert.Len(t, items, 1)
	assert.NoError(t, exporter.writeTable(items, "logs"))
}
