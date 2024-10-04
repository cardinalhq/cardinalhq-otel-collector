// Copyright 2024 CardinalHQ, Inc
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

package chqdecoratorprocessor

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// Test the logProcessor processLogs function with conditions and statements
func TestLogProcessorProcessLogs_ConditionStatementEvaluation(t *testing.T) {
	// Create a logger
	logger := zap.NewNop()

	// Create processor settings
	set := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}

	// Create mock config with log_statements
	cfg := &Config{
		LogsConfig: LogsConfig{
			Transforms: []ContextStatement{
				{
					Context: "log",
					Conditions: []string{
						`IsMap(body) and body["object"] != nil`,
					},
					Statements: []string{
						`set(body, attributes["http.route"])`,
					},
				},
			},
		},
	}

	// Create the log processor
	lp, err := newLogsProcessor(set, cfg)
	assert.NoError(t, err)

	// Create mock log data that matches the condition
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := sl.LogRecords().AppendEmpty()

	// Simulate a log body with a map containing 'object' key
	logRecord.Body().SetEmptyMap()
	logRecord.Body().Map().PutStr("object", "some_object")
	logRecord.Attributes().PutStr("http.route", "/test/route")

	// Process the logs
	processedLogs, err := lp.processLogs(context.Background(), logs)
	assert.NoError(t, err)

	// Check if the statement was executed (i.e., body should be set to "/test/route")
	processedLogRecord := processedLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	processedBody := processedLogRecord.Body()
	assert.Equal(t, "/test/route", processedBody.AsString())

	// Create log data that does not match the condition
	logsNoMatch := plog.NewLogs()
	rlNoMatch := logsNoMatch.ResourceLogs().AppendEmpty()
	slNoMatch := rlNoMatch.ScopeLogs().AppendEmpty()
	logRecordNoMatch := slNoMatch.LogRecords().AppendEmpty()

	// Simulate a log body without the 'object' key
	logRecordNoMatch.Body().SetEmptyMap()
	logRecordNoMatch.Body().Map().PutStr("another_key", "no_object")

	// Process logs that should not match the condition
	processedLogsNoMatch, err := lp.processLogs(context.Background(), logsNoMatch)
	assert.NoError(t, err)

	// Check that the body was not modified, meaning the statement wasn't applied
	processedLogRecordNoMatch := processedLogsNoMatch.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	anotherKeyVal, _ := processedLogRecordNoMatch.Body().Map().Get("another_key")
	noObject := anotherKeyVal.Str()
	assert.Equal(t, "no_object", noObject)
}
