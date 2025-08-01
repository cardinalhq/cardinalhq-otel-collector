// Copyright 2024-2025 CardinalHQ, Inc
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

package datadogreceiver

import (
	"encoding/json"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestSplitTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     string
		expected map[string]string
	}{
		{
			"no tags",
			"",
			map[string]string{},
		},
		{
			"one tag",
			"tag1:value1",
			map[string]string{
				"tag1": "value1",
			},
		},
		{
			"multiple tags",
			"tag1:value1,tag2:value2,tag3:value3",
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		{
			"multiple tags with spaces",
			"tag1:value1, tag2:value2,   tag3:value3",
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		{
			"tags without values",
			"tag1:,tag2:value2,tag3",
			map[string]string{
				"tag2": "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, splitTags(tt.tags))
		})
	}
}

func TestToSeverity(t *testing.T) {
	tests := []struct {
		name           string
		severityString string
		expectedNumber plog.SeverityNumber
		expectedString string
	}{
		{
			"error",
			"error",
			plog.SeverityNumberError,
			"Error",
		},
		{
			"warn",
			"warn",
			plog.SeverityNumberWarn,
			"Warn",
		},
		{
			"info",
			"info",
			plog.SeverityNumberInfo,
			"Info",
		},
		{
			"debug",
			"debug",
			plog.SeverityNumberDebug,
			"Debug",
		},
		{
			"trace",
			"trace",
			plog.SeverityNumberTrace,
			"Trace",
		},
		{
			"unspecified",
			"unknown",
			plog.SeverityNumberUnspecified,
			"Unspecified",
		},
		{
			"empty",
			"",
			plog.SeverityNumberUnspecified,
			"Unspecified",
		},
		{
			"alice",
			"alice",
			plog.SeverityNumberUnspecified,
			"Unspecified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			number, str := toSeverity(tt.severityString)
			assert.Equal(t, tt.expectedNumber, number)
			assert.Equal(t, tt.expectedString, str)
		})
	}
}

func TestSplitTagSlice(t *testing.T) {
	tests := []struct {
		name     string
		tags     []string
		expected map[string]string
	}{
		{
			"no tags",
			[]string{},
			map[string]string{},
		},
		{
			"one tag",
			[]string{"tag1:value1"},
			map[string]string{
				"tag1": "value1",
			},
		},
		{
			"multiple tags",
			[]string{"tag1:value1", "tag2:value2", "tag3:value3"},
			map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
		},
		{
			"tags with empty values",
			[]string{"tag1:", "tag2:value2", "tag3"},
			map[string]string{
				"tag2": "value2",
			},
		},
		{
			"tags with spaces",
			[]string{"tag1:value1", "tag2:value 2", "tag3:value3", "tag4:value4", "tag5:value5"},
			map[string]string{
				"tag1": "value1",
				"tag2": "value 2",
				"tag3": "value3",
				"tag4": "value4",
				"tag5": "value5",
			},
		},
		{
			"tags with commas",
			[]string{"tag1:value1", "tag2:value,2"},
			map[string]string{
				"tag1": "value1",
				"tag2": "value,2",
			},
		},
		{
			"tags with colons",
			[]string{"tag1:value1", "tag2:value:2"},
			map[string]string{
				"tag1": "value1",
				"tag2": "value:2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, splitTagSlice(tt.tags))
		})
	}
}

func TestSplitLogs_MultilineExceptionHandling(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680000000,host:app-server-1",
			Message:  "2025-01-31 01:38:02.740 ERROR --- [lt-dispatcher-6] Error in sketch merge: No space left on device",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "at akka.stream.impl.fusing.ActorGraphInterpreter.processEvent(ActorGraphInterpreter.scala:800)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680002000,host:app-server-1",
			Message:  "at com.cardinal.microbatch.jobs.metrics.rollups.SketchMergeStage.onPush(SketchMergeStage.scala:167)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680020000,host:app-server-1",
			Message:  "2025-01-31 01:38:22.332 INFO --- Some non-error log message",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)
	group := groupedLogs[0]

	assert.Equal(t, "order-service", group.Service)
	assert.Equal(t, "app-server-1", group.Hostname)

	assert.Len(t, group.Messages, 2)

	expectedException := `2025-01-31 01:38:02.740 ERROR --- [lt-dispatcher-6] Error in sketch merge: No space left on device
at akka.stream.impl.fusing.ActorGraphInterpreter.processEvent(ActorGraphInterpreter.scala:800)
at com.cardinal.microbatch.jobs.metrics.rollups.SketchMergeStage.onPush(SketchMergeStage.scala:167)`

	assert.Equal(t, expectedException, group.Messages[0].Body)

	// Verify the second separate log message
	assert.Equal(t, "2025-01-31 01:38:22.332 INFO --- Some non-error log message", group.Messages[1].Body)
}

func TestSplitLogs_GoStackTraceHandling(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680000000,host:app-server-1",
			Message:  "2025-01-31T05:03:29.823Z        info    pitbullprocessor@v0.117.0/metrics.go:158        Updating metrics transformations        {\"kind\": \"processor\", \"name\": \"pitbull/bfe5bc8b-58f6-4be7-8b36-d01c21455c09\", \"pipeline\": \"metrics/bfe5bc8b-58f6-4be7-8b36-d01c21455c09\", \"num_decorators\": 1}",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "2025-01-31T05:03:29.824Z        info    pitbullprocessor@v0.117.0/traces.go:88  Updating trace transformations  {\"kind\": \"processor\", \"name\": \"pitbull/adbe59e9-ab9e-412b-ae6a-97f549abb8ff\", \"pipeline\": \"traces/adbe59e9-ab9e-412b-ae6a-97f549abb8ff\", \"num_decorators\": 4}",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680002000,host:app-server-1",
			Message:  "fatal error: concurrent map writes",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680003000,host:app-server-1",
			Message:  "goroutine 1397 [running]:",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680004000,host:app-server-1",
			Message:  "github.com/cardinalhq/oteltools/pkg/graph.(*ResourceEntityCache).provisionEntities(0xc001637b80, {0xc006ac0738?, 0xc000ea811c?}, 0xc009612270)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680005000,host:app-server-1",
			Message:  "        github.com/cardinalhq/oteltools@v0.8.7/pkg/graph/entitycache.go:148 +0x25b",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680006000,host:app-server-1",
			Message:  "github.com/cardinalhq/oteltools/pkg/graph.(*ResourceEntityCache).ProvisionResourceAttributes(0xc001637b80, {0xc006ac0738?, 0xc000ea811c?})",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680020000,host:app-server-1",
			Message:  "go.opentelemetry.io/collector/processor/processorhelper.NewMetrics.func1({0x6015e28, 0xc0096120c0}, {0xc00137c4e0?, 0xc000ea811c?})",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)
	group := groupedLogs[0]

	assert.Equal(t, "order-service", group.Service)
	assert.Equal(t, "app-server-1", group.Hostname)

	assert.Len(t, group.Messages, 3)

	expectedException := `fatal error: concurrent map writes
goroutine 1397 [running]:
github.com/cardinalhq/oteltools/pkg/graph.(*ResourceEntityCache).provisionEntities(0xc001637b80, {0xc006ac0738?, 0xc000ea811c?}, 0xc009612270)
        github.com/cardinalhq/oteltools@v0.8.7/pkg/graph/entitycache.go:148 +0x25b
github.com/cardinalhq/oteltools/pkg/graph.(*ResourceEntityCache).ProvisionResourceAttributes(0xc001637b80, {0xc006ac0738?, 0xc000ea811c?})
go.opentelemetry.io/collector/processor/processorhelper.NewMetrics.func1({0x6015e28, 0xc0096120c0}, {0xc00137c4e0?, 0xc000ea811c?})`

	assert.Equal(t, expectedException, group.Messages[2].Body)
}

func TestSplitLogs_ConsecutiveErrorsWithStackTrace(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680000000,host:app-server-1",
			Message:  "2025-01-31T05:42:04.129Z        error   internal/queue_sender.go:103    Exporting failed. Dropping data.        {\"kind\": \"exporter\", \"data_type\": \"metrics\", \"name\": \"prometheusremotewrite/933f4a19-24ec-4b8d-ae54-1e7807f050b9\", \"error\": \"Permanent error: Permanent error: Permanent error: remote write returned HTTP status 400 Bad Request; err = %!w(<nil>): too old sample\", \"dropped_items\": 1}",
			Hostname: "app-server-1",
			Service:  "otel-collector",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680000100,host:app-server-1",
			Message:  "go.opentelemetry.io/collector/exporter/exporterhelper/internal.NewQueueSender.func1",
			Hostname: "app-server-1",
			Service:  "otel-collector",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680000200,host:app-server-1",
			Message:  "go.opentelemetry.io/collector/exporter/internal/queue.(*Consumers[...]).Start.func1",
			Hostname: "app-server-1",
			Service:  "otel-collector",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680000300,host:app-server-1",
			Message:  "2025-01-31T05:42:04.130Z        error   internal/queue_sender.go:103    Exporting failed. Dropping data.        {\"kind\": \"exporter\", \"data_type\": \"metrics\", \"name\": \"prometheusremotewrite/933f4a19-24ec-4b8d-ae54-1e7807f050b9\", \"error\": \"Permanent error: Permanent error: Permanent error: remote write returned HTTP status 400 Bad Request; err = %!w(<nil>): too old sample\", \"dropped_items\": 1}",
			Hostname: "app-server-1",
			Service:  "otel-collector",
		},
		{
			DDSource: "go",
			DDTags:   "timestamp:1706680000400,host:app-server-1",
			Message:  "go.opentelemetry.io/collector/exporter/exporterhelper/internal.NewQueueSender.func1",
			Hostname: "app-server-1",
			Service:  "otel-collector",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)

	group := groupedLogs[0]
	assert.Equal(t, "otel-collector", group.Service)
	assert.Equal(t, "app-server-1", group.Hostname)

	assert.Len(t, group.Messages, 2)

	expectedError1 := `2025-01-31T05:42:04.129Z        error   internal/queue_sender.go:103    Exporting failed. Dropping data.        {"kind": "exporter", "data_type": "metrics", "name": "prometheusremotewrite/933f4a19-24ec-4b8d-ae54-1e7807f050b9", "error": "Permanent error: Permanent error: Permanent error: remote write returned HTTP status 400 Bad Request; err = %!w(<nil>): too old sample", "dropped_items": 1}
go.opentelemetry.io/collector/exporter/exporterhelper/internal.NewQueueSender.func1
go.opentelemetry.io/collector/exporter/internal/queue.(*Consumers[...]).Start.func1`

	expectedError2 := `2025-01-31T05:42:04.130Z        error   internal/queue_sender.go:103    Exporting failed. Dropping data.        {"kind": "exporter", "data_type": "metrics", "name": "prometheusremotewrite/933f4a19-24ec-4b8d-ae54-1e7807f050b9", "error": "Permanent error: Permanent error: Permanent error: remote write returned HTTP status 400 Bad Request; err = %!w(<nil>): too old sample", "dropped_items": 1}
go.opentelemetry.io/collector/exporter/exporterhelper/internal.NewQueueSender.func1`

	assert.Equal(t, expectedError1, group.Messages[0].Body)
	assert.Equal(t, expectedError2, group.Messages[1].Body)
}

func TestSplitLogs_PythonStackTraceHandling(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680000000,host:app-server-1",
			Message:  "2025-01-31T10:00:01.123Z    info    app.main    Starting application...",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "2025-01-31T10:00:02.456Z    debug   app.worker  Processing request ID=abc123",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680002000,host:app-server-1",
			Message:  "2025-01-31T10:00:03.789Z    warn    app.cache   Cache miss for key=user:42",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003000,host:app-server-1",
			Message:  "2025-01-31T10:00:05.000Z    error   app.database  Database connection lost, retrying...",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003100,host:app-server-1",
			Message:  "Traceback (most recent call last):",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003200,host:app-server-1",
			Message:  "  File \"database.py\", line 45, in connect",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003300,host:app-server-1",
			Message:  "    conn = psycopg2.connect(dsn)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003400,host:app-server-1",
			Message:  "  File \"/usr/lib/python3.10/site-packages/psycopg2/__init__.py\", line 126, in connect",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003500,host:app-server-1",
			Message:  "    raise OperationalError(\"could not connect to server\")",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "python",
			DDTags:   "timestamp:1706680003600,host:app-server-1",
			Message:  "psycopg2.OperationalError: could not connect to server",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)
	group := groupedLogs[0]

	assert.Equal(t, "order-service", group.Service)
	assert.Equal(t, "app-server-1", group.Hostname)

	assert.Len(t, group.Messages, 4)

	expectedException := `2025-01-31T10:00:05.000Z    error   app.database  Database connection lost, retrying...
Traceback (most recent call last):
  File "database.py", line 45, in connect
    conn = psycopg2.connect(dsn)
  File "/usr/lib/python3.10/site-packages/psycopg2/__init__.py", line 126, in connect
    raise OperationalError("could not connect to server")
psycopg2.OperationalError: could not connect to server`

	assert.Equal(t, expectedException, group.Messages[3].Body)

	assert.Equal(t, "2025-01-31T10:00:01.123Z    info    app.main    Starting application...", group.Messages[0].Body)

	assert.Equal(t, "2025-01-31T10:00:02.456Z    debug   app.worker  Processing request ID=abc123", group.Messages[1].Body)
	assert.Equal(t, "2025-01-31T10:00:03.789Z    warn    app.cache   Cache miss for key=user:42", group.Messages[2].Body)
}

func TestSplitLogs_NodeJSStackTraceHandling(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "nodejs",
			DDTags:   "timestamp:1706705701200,host:node-server-1",
			Message:  "2025-01-31T12:15:01.200Z    info    server.main    Server started on port 8080",
			Hostname: "node-server-1",
			Service:  "server",
		},
		{
			DDSource: "nodejs",
			DDTags:   "timestamp:1706705705123,host:node-server-1",
			Message:  "2025-01-31T12:15:05.123Z    error   db.connector   Database query failed",
			Hostname: "node-server-1",
			Service:  "database",
		},
		{
			DDSource: "nodejs",
			DDTags:   "timestamp:1706705705124,host:node-server-1",
			Message:  "Error: Connection lost: The server closed the connection.",
			Hostname: "node-server-1",
			Service:  "database",
		},
		{
			DDSource: "nodejs",
			DDTags:   "timestamp:1706705705125,host:node-server-1",
			Message:  "    at Connection._handleFatalError (/app/node_modules/mysql/lib/Connection.js:123 +0x25b)",
			Hostname: "node-server-1",
			Service:  "database",
		},
		{
			DDSource: "nodejs",
			DDTags:   "timestamp:1706705705126,host:node-server-1",
			Message:  "    at Connection.end (/app/node_modules/mysql/lib/Connection.js:148 +0x48)",
			Hostname: "node-server-1",
			Service:  "database",
		},
		{
			DDSource: "nodejs",
			DDTags:   "timestamp:1706705705127,host:node-server-1",
			Message:  "    at processTicksAndRejections (node:internal/process/task_queues:95:5)",
			Hostname: "node-server-1",
			Service:  "database",
		},
	}

	got := ddr.splitLogs(logs, "apiKey")
	require.Len(t, got, 2)

	expected := []groupedLogs{
		{
			Service:  "server",
			Hostname: "node-server-1",
			DDSource: "nodejs",
			Tags: map[string]string{
				"timestamp": "1706705701200",
				"host":      "node-server-1",
			},
			Messages: []Message{
				{
					Timestamp: 1706705701200000000,
					Body:      "2025-01-31T12:15:01.200Z    info    server.main    Server started on port 8080",
				},
			},
		},
		{
			Service:  "database",
			Hostname: "node-server-1",
			DDSource: "nodejs",
			Tags: map[string]string{
				"timestamp": "1706705705123",
				"host":      "node-server-1",
			},
			Messages: []Message{
				{
					Timestamp: 1706705705123000000,
					Body:      "2025-01-31T12:15:05.123Z    error   db.connector   Database query failed\nError: Connection lost: The server closed the connection.\n    at Connection._handleFatalError (/app/node_modules/mysql/lib/Connection.js:123 +0x25b)\n    at Connection.end (/app/node_modules/mysql/lib/Connection.js:148 +0x48)\n    at processTicksAndRejections (node:internal/process/task_queues:95:5)",
				},
			},
		},
	}

	assert.ElementsMatch(t, expected, got)
}

func TestSplitLogs_NoValidStartLines_ShouldBeIndividualMessages(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "custom",
			DDTags:   "timestamp:1706680000000,host:server-1",
			Message:  "This is a normal log message.",
			Hostname: "server-1",
			Service:  "test-service",
		},
		{
			DDSource: "custom",
			DDTags:   "timestamp:1706680001000,host:server-1",
			Message:  "This should be separate because it lacks a timestamp.",
			Hostname: "server-1",
			Service:  "test-service",
		},
		{
			DDSource: "custom",
			DDTags:   "timestamp:1706680002000,host:server-1",
			Message:  "Also a separate message without a timestamp.",
			Hostname: "server-1",
			Service:  "test-service",
		},
		{
			DDSource: "custom",
			DDTags:   "timestamp:1706680003000,host:server-1",
			Message:  "Yet another standalone message.",
			Hostname: "server-1",
			Service:  "test-service",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)
	group := groupedLogs[0]

	assert.Equal(t, "test-service", group.Service)
	assert.Equal(t, "server-1", group.Hostname)

	assert.Len(t, group.Messages, 4)

	assert.Equal(t, "This is a normal log message.", group.Messages[0].Body)
	assert.Equal(t, "This should be separate because it lacks a timestamp.", group.Messages[1].Body)
	assert.Equal(t, "Also a separate message without a timestamp.", group.Messages[2].Body)
	assert.Equal(t, "Yet another standalone message.", group.Messages[3].Body)
}

func TestSplitLogs_StackTraceAtStart_ShouldGroupCorrectly(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680000000,host:app-server-1",
			Message:  "at com.example.Class.method(Class.java:123)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "at com.example.OtherClass.anotherMethod(OtherClass.java:456)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "at com.example.OtherClass.anotherMethod(OtherClass.java:123)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680002000,host:app-server-1",
			Message:  "2025-01-31 01:38:02.740 ERROR --- Something failed",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)
	group := groupedLogs[0]

	assert.Len(t, group.Messages, 2)
	expectedStackTrace := `at com.example.Class.method(Class.java:123)
at com.example.OtherClass.anotherMethod(OtherClass.java:456)
at com.example.OtherClass.anotherMethod(OtherClass.java:123)`

	assert.Equal(t, expectedStackTrace, group.Messages[0].Body)
	assert.Equal(t, "2025-01-31 01:38:02.740 ERROR --- Something failed", group.Messages[1].Body)
}

func TestSplitLogs_OnlyStackTrace_ShouldGroupCorrectly(t *testing.T) {
	ddr := &datadogReceiver{}
	logs := []DDLog{
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680000000,host:app-server-1",
			Message:  "at com.example.Class.method(Class.java:123)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "at com.example.OtherClass.anotherMethod(OtherClass.java:456)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
		{
			DDSource: "java",
			DDTags:   "timestamp:1706680001000,host:app-server-1",
			Message:  "at com.example.OtherClass.anotherMethod(OtherClass.java:123)",
			Hostname: "app-server-1",
			Service:  "order-service",
		},
	}

	groupedLogs := ddr.splitLogs(logs, "apiKey")

	assert.Len(t, groupedLogs, 1)
	group := groupedLogs[0]

	assert.Len(t, group.Messages, 1)
	expectedStackTrace := `at com.example.Class.method(Class.java:123)
at com.example.OtherClass.anotherMethod(OtherClass.java:456)
at com.example.OtherClass.anotherMethod(OtherClass.java:123)`

	assert.Equal(t, expectedStackTrace, group.Messages[0].Body)
}

func TestJsonParsing(t *testing.T) {
	payload := `{
		"time": "2025-06-28T00:29:21.820871858Z",
		"level": "INFO",
		"msg": "session found",
		"application": "cashew",
		"environment": "prod",
		"services": "session",
		"session_id": "9953b572-c769-4928-b3b3-9927d065e386",
		"correlation_id": "9d1c8ca8-cb69-4a48-9cb0-b403791d8e22"
	}`

	var parsed map[string]interface{}
	err := json.Unmarshal([]byte(payload), &parsed)
	require.NoError(t, err, "expected JSON payload to parse correctly")

	body := pcommon.NewValueEmpty()
	attrMap := body.SetEmptyMap()

	convertToOtelMap(parsed, attrMap)

	// Validate key fields
	get, b := attrMap.Get("level")
	require.True(t, b, "expected 'level' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "INFO", get.Str(), "expected 'level' to be 'INFO'")
	get, b = attrMap.Get("msg")
	require.True(t, b, "expected 'msg' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "session found", get.Str(), "expected 'msg' to be 'session found'")
	get, b = attrMap.Get("application")
	require.True(t, b, "expected 'application' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "cashew", get.Str(), "expected 'application' to be 'cashew'")
	get, b = attrMap.Get("environment")
	require.True(t, b, "expected 'environment' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "prod", get.Str(), "expected 'environment' to be 'prod'")
	get, b = attrMap.Get("services")
	require.True(t, b, "expected 'services' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "session", get.Str(), "expected 'services' to be 'session'")
	get, b = attrMap.Get("session_id")
	require.True(t, b, "expected 'session_id' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "9953b572-c769-4928-b3b3-9927d065e386", get.Str(), "expected 'session_id' to match")
	get, b = attrMap.Get("correlation_id")
	require.True(t, b, "expected 'correlation_id' attribute to be present")
	require.NotNil(t, get)
	require.Equal(t, "9d1c8ca8-cb69-4a48-9cb0-b403791d8e22", get.Str(), "expected 'correlation_id' to match")
}
