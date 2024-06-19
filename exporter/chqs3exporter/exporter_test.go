// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/timebox"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
)

var testLogs = []byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"_sourceCategory","value":{"stringValue":"logfile"}},{"key":"_sourceHost","value":{"stringValue":"host"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"observedTimeUnixNano":"1654257420681895000","body":{"stringValue":"2022-06-03 13:57:00.62739 +0200 CEST m=+14.018296742 log entry14"},"attributes":[{"key":"log.file.path_resolved","value":{"stringValue":"logwriter/data.log"}}],"traceId":"","spanId":""}]}],"schemaUrl":"https://opentelemetry.io/schemas/1.6.1"}]}`)

type TestWriter struct {
	t *testing.T
}

func (testWriter *TestWriter) writeBuffer(_ context.Context, now time.Time, buf io.Reader, _ *Config, _ string, _ string, _ map[string]string, customerID string) error {
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

func dummyTelemetry() *exporterTelemetry {
	meter := metric.NewMeterProvider()
	m, _ := meter.Meter("test").Int64Counter("test")
	return &exporterTelemetry{
		filesWritten:    m,
		datapointTooOld: m,
	}
}

func getLogExporter(t *testing.T) *s3Exporter {
	config := createDefaultConfig().(*Config)
	config.Timeboxes.Logs.Interval = 10
	config.Timeboxes.Logs.OpenIntervalCount = 1
	config.Timeboxes.Logs.GracePeriod = 0
	bufferFactory := timebox.NewMemoryBufferFactory()
	exporter := &s3Exporter{
		config:     config,
		dataWriter: &TestWriter{t},
		logger:     zap.NewNop(),
		tb:         table.NewTableTranslator(),
		logs:       timebox.NewTimeboxImpl[string, *TimeboxEntry](bufferFactory, config.Timeboxes.Logs.Interval, config.Timeboxes.Logs.OpenIntervalCount, config.Timeboxes.Logs.GracePeriod),
		telemetry:  dummyTelemetry(),
	}
	return exporter
}

func TestLog(t *testing.T) {
	logs := getTestLogs(t)
	firstTS, lastTS := getTimestamps(logs)
	log.Printf("firstTS: %d, lastTS: %d", firstTS, lastTS)
	exporter := getLogExporter(t)
	oldest, err := exporter.consumeLogs(firstTS, logs, "default")
	assert.NoError(t, err)
	assert.NotZero(t, oldest)
	items := exporter.logs.Closed("default", oldest, &TimeboxEntry{})
	assert.Len(t, items, 0)
	items = exporter.logs.Closed("default", lastTS+200_000, &TimeboxEntry{})
	assert.Len(t, items, 1)
	assert.NoError(t, exporter.writeTable(items, "logs", "default"))
}

func getTimestamps(logs plog.Logs) (int64, int64) {
	var firstTS pcommon.Timestamp
	var lastTS pcommon.Timestamp
	logs.ResourceLogs().RemoveIf(func(resourceLogs plog.ResourceLogs) bool {
		resourceLogs.ScopeLogs().RemoveIf(func(scopeLogs plog.ScopeLogs) bool {
			scopeLogs.LogRecords().RemoveIf(func(logRecord plog.LogRecord) bool {
				ts := logRecord.Timestamp() / 1_000_000
				if ts == 0 {
					ts = logRecord.ObservedTimestamp() / 1_000_000
				}
				if firstTS == 0 || ts < firstTS {
					firstTS = ts
				}
				if ts > lastTS {
					lastTS = ts
				}
				return false
			})
			return false
		})
		return false
	})
	return int64(firstTS), int64(lastTS)
}
