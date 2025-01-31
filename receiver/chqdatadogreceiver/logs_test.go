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
	"testing"

	"github.com/stretchr/testify/assert"
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
