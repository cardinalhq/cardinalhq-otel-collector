// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/translation/table"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/boxer"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
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
type mockFileWriter struct {
	writeBufferFunc func(ctx context.Context, now time.Time, buf io.Reader, config *Config, prefix string, format string, metadata map[string]string, customerID string) error
}

func (m *mockFileWriter) writeBuffer(ctx context.Context, now time.Time, buf io.Reader, config *Config, prefix string, format string, metadata map[string]string, customerID string) error {
	if m.writeBufferFunc != nil {
		return m.writeBufferFunc(ctx, now, buf, config, prefix, format, metadata, customerID)
	}
	return nil
}

var _ filewriter = (*mockFileWriter)(nil)

func TestUpload(t *testing.T) {
	// Create a mock file writer
	mockWriter := &mockFileWriter{}

	// Create a test file

	tmpfile, err := os.CreateTemp("", "example")
	assert.NoError(t, err)
	defer tmpfile.Close()
	defer func() {
		err := os.Remove(tmpfile.Name())
		assert.NoError(t, err)
	}()

	// Create a test configuration
	config := &Config{
		Buffering: BufferingConfig{
			Directory: "/tmp",
			Type:      "disk",
		},
	}

	// Create a test metadata
	metadata := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	// Create a test customer ID and interval
	customerID := "test-customer"
	interval := int64(1234567890)

	buffer := boxer.NewMemoryBuffer()
	opts := []boxer.BoxerOptions{
		boxer.WithInterval(time.Second),
		boxer.WithBufferStorage(buffer),
	}
	id := component.MustNewIDWithName("exporter", "test-name")
	box, err := boxer.BoxerFor(config.Buffering.Directory, component.KindExporter, id, "logs", opts...)
	assert.NoError(t, err)

	// Create the s3Exporter instance
	exporter := &s3Exporter{
		config:        config,
		id:            id,
		tb:            table.NewTableTranslator(),
		boxer:         box,
		metadata:      metadata,
		telemetryType: "logs",
		logger:        zap.NewNop(),
	}

	// Set the writeBufferFunc of the mock file writer to validate the arguments
	var capturedConfig *Config
	var capturedPrefix string
	var capturedFormat string
	var capturedMetadata map[string]string
	var capturedCustomerID string
	var capturedData []byte
	mockWriter.writeBufferFunc = func(ctx context.Context, _ time.Time, file io.Reader, config *Config, prefix string, format string, metadata map[string]string, customerID string) error {
		capturedConfig = config
		capturedPrefix = prefix
		capturedFormat = format
		capturedMetadata = metadata
		capturedCustomerID = customerID

		data, err := io.ReadAll(file)
		assert.NoError(t, err)
		capturedData = data

		return nil
	}

	testdata := []byte("test data")
	_, err = tmpfile.Write(testdata)
	assert.NoError(t, err)

	// Call the upload function
	err = exporter.upload(tmpfile, mockWriter, customerID, interval)

	// Assert that the writeBufferFunc was called with the correct arguments
	assert.NoError(t, err)
	assert.Equal(t, config, capturedConfig)
	assert.Equal(t, "logs_1234567890000", capturedPrefix)
	assert.Equal(t, "parquet", capturedFormat)
	assert.Equal(t, metadata, capturedMetadata)
	assert.Equal(t, customerID, capturedCustomerID)
	assert.Equal(t, testdata, capturedData)
}

func TestFilesize(t *testing.T) {
	file, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	defer os.Remove(file.Name())
	defer file.Close()

	data := []byte("Hello, World!")
	_, err = file.Write(data)
	assert.NoError(t, err)

	size, err := filesize(file)
	assert.NoError(t, err)

	expectedSize := int64(len(data))
	assert.Equal(t, expectedSize, size)
}

func TestFilesizeError(t *testing.T) {
	_, err := filesize(nil)
	assert.Error(t, err)
}
