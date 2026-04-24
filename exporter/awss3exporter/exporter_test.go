// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/notify"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/upload"
)

var (
	s3PrefixKey    = "_sourceHost"
	s3BucketKey    = "_sourceBucket"
	overridePrefix = "host"
	overrideBucket = "my-bucket"
	testLogs       = fmt.Sprintf(`{"resourceLogs":[{"resource":{"attributes":[{"key":"_sourceCategory","value":{"stringValue":"logfile"}},{"key":"%s","value":{"stringValue":"%s"}},{"key":"%s","value":{"stringValue":"%s"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"observedTimeUnixNano":"1654257420681895000","body":{"stringValue":"2022-06-03 13:57:00.62739 +0200 CEST m=+14.018296742 log entry14"},"attributes":[{"key":"log.file.path_resolved","value":{"stringValue":"logwriter/data.log"}}]}]}],"schemaUrl":"https://opentelemetry.io/schemas/1.6.1"}]}`, s3PrefixKey, overridePrefix, s3BucketKey, overrideBucket) //nolint:gocritic //sprintfQuotedString for JSON
)

type testWriter struct {
	t            *testing.T
	expectedOpts *upload.UploadOptions
}

func (testWriter *testWriter) Upload(_ context.Context, buf []byte, uploadOpts *upload.UploadOptions) error {
	assert.JSONEq(testWriter.t, testLogs, string(buf))
	assert.Equal(testWriter.t, testWriter.expectedOpts, uploadOpts)
	return nil
}

func getTestLogs(tb testing.TB) plog.Logs {
	logsMarshaler := plog.JSONUnmarshaler{}
	logs, err := logsMarshaler.UnmarshalLogs([]byte(testLogs))
	assert.NoError(tb, err, "Can't unmarshal testing the logs data -> %s", err)
	return logs
}

func getLogExporter(t *testing.T) *s3Exporter {
	marshaler, _ := newMarshaler("otlp_json", zap.NewNop())
	exporter := &s3Exporter{
		config:    createDefaultConfig().(*Config),
		uploader:  &testWriter{t: t, expectedOpts: &upload.UploadOptions{OverridePrefix: ""}},
		logger:    zap.NewNop(),
		marshaler: marshaler,
	}
	return exporter
}

func TestLog(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporter(t)
	assert.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}

func getLogExporterWithResourceAttrs(t *testing.T) *s3Exporter {
	marshaler, _ := newMarshaler("otlp_json", zap.NewNop())
	config := createDefaultConfig().(*Config)
	config.ResourceAttrsToS3.S3Prefix = s3PrefixKey
	exporter := &s3Exporter{
		config:    config,
		uploader:  &testWriter{t: t, expectedOpts: &upload.UploadOptions{OverridePrefix: overridePrefix}},
		logger:    zap.NewNop(),
		marshaler: marshaler,
	}
	return exporter
}

func TestLogWithResourceAttrs(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporterWithResourceAttrs(t)
	assert.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}

func getLogExporterWithBucketAndPrefixAttrs(t *testing.T) *s3Exporter {
	marshaler, _ := newMarshaler("otlp_json", zap.NewNop())
	config := createDefaultConfig().(*Config)
	config.ResourceAttrsToS3.S3Bucket = s3BucketKey
	config.ResourceAttrsToS3.S3Prefix = s3PrefixKey
	exporter := &s3Exporter{
		config:    config,
		uploader:  &testWriter{t: t, expectedOpts: &upload.UploadOptions{OverrideBucket: overrideBucket, OverridePrefix: overridePrefix}},
		logger:    zap.NewNop(),
		marshaler: marshaler,
	}
	return exporter
}

func TestLogWithBucketAndPrefixAttrs(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporterWithBucketAndPrefixAttrs(t)
	assert.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}

// stubExporterNotifier counts Shutdown invocations so the exporter-level
// wiring test can verify s3Exporter.shutdown forwards the call. Enqueue is
// unused here; the upload-manager test covers the Enqueue path.
type stubExporterNotifier struct {
	shutdownCalls atomic.Int32
}

func (*stubExporterNotifier) Enqueue(_ context.Context, _ notify.Event) bool { return false }

func (s *stubExporterNotifier) Shutdown(_ context.Context) error {
	s.shutdownCalls.Add(1)
	return nil
}

func TestExporterShutdownCallsNotifier(t *testing.T) {
	t.Parallel()

	stub := &stubExporterNotifier{}
	e := &s3Exporter{
		config:   &Config{},
		logger:   zap.NewNop(),
		notifier: stub,
	}

	require.NoError(t, e.shutdown(t.Context()))
	assert.EqualValues(t, 1, stub.shutdownCalls.Load(),
		"s3Exporter.shutdown must forward to the notifier exactly once")
}

func TestExporterShutdownNilNotifierIsNoop(t *testing.T) {
	t.Parallel()

	// Covers the code path where Start failed before the notifier was built
	// or where the exporter instance was constructed for a test that skips
	// start().
	e := &s3Exporter{
		config: &Config{},
		logger: zap.NewNop(),
	}
	require.NoError(t, e.shutdown(t.Context()))
}
