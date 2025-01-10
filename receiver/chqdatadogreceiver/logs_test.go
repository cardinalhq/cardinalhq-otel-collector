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
