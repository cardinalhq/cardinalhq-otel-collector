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

package chqs3exporter

import (
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/boxer"
)

func TestCustomerIDFromMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected string
	}{
		{
			"customer_id field",
			map[string]any{
				translate.CardinalFieldCustomerID: "12345",
			},
			"12345",
		},
		{
			"no customer_id field",
			map[string]any{
				"foo": "value",
			},
			"_default",
		},
		{
			"non-string customer_id field",
			map[string]any{
				translate.CardinalFieldCustomerID: 12345,
			},
			"_default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := customerIDFromMap(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCollectorIDFromMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected string
	}{
		{
			"collector_id field",
			map[string]any{
				translate.CardinalFieldCollectorID: "12345",
			},
			"12345",
		},
		{
			"no collector_id field",
			map[string]any{
				"foo": "value",
			},
			"_default",
		},
		{
			"non-string collector_id field",
			map[string]any{
				translate.CardinalFieldCollectorID: 12345,
			},
			"_default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collectorIDFromMap(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimestampFromMap(t *testing.T) {
	tests := []struct {
		name          string
		input         map[string]any
		expectedTime  time.Time
		expectedFound bool
	}{
		{
			"timestamp field",
			map[string]any{
				translate.CardinalFieldTimestamp: int64(1000),
			},
			time.UnixMilli(1000),
			true,
		},
		{
			"no timestamp field",
			map[string]any{
				"foo": "value",
			},
			time.Time{},
			false,
		},
		{
			"non-int64 timestamp field",
			map[string]any{
				translate.CardinalFieldTimestamp: "1000",
			},
			time.Time{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := timestampFromMap(tt.input)
			assert.Equal(t, tt.expectedTime, result)
			if found {
				assert.Equal(t, tt.expectedTime, result)
			}
		})
	}
}

func TestPartitionTableByCustomerIDAndInterval(t *testing.T) {
	ts := int64(1000)

	tests := []struct {
		name           string
		input          []map[string]any
		expected       map[string]map[int64][]map[string]any
		expectedTagmap map[string]map[int64]map[string]any
	}{
		{
			"empty table",
			[]map[string]any{},
			map[string]map[int64][]map[string]any{},
			map[string]map[int64]map[string]any{},
		},
		{
			"single entry",
			[]map[string]any{
				{
					translate.CardinalFieldCustomerID:  "alice",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   ts,
					"foo":                              "value",
				},
			},
			map[string]map[int64][]map[string]any{
				"alice/12345": {
					1: []map[string]any{
						{
							translate.CardinalFieldCustomerID:  "alice",
							translate.CardinalFieldCollectorID: "12345",
							translate.CardinalFieldTimestamp:   ts,
							"foo":                              "value",
						},
					},
				},
			},
			map[string]map[int64]map[string]any{
				"alice/12345": {
					1: {
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   ts,
						"foo":                              "value",
					},
				},
			},
		},
		{
			"multiple entries",
			[]map[string]any{
				{
					translate.CardinalFieldCustomerID:  "alice",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   ts,
					"item1":                            "value1",
				},
				{
					translate.CardinalFieldCustomerID:  "alice",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   ts * 3,
					"item2":                            "value2",
				},
				{
					translate.CardinalFieldCustomerID:  "bob",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   ts * 2,
					"item3":                            "value3",
				},
			},
			map[string]map[int64][]map[string]any{
				"alice/12345": {
					1: []map[string]any{
						{
							translate.CardinalFieldCustomerID:  "alice",
							translate.CardinalFieldCollectorID: "12345",
							translate.CardinalFieldTimestamp:   ts,
							"item1":                            "value1",
						},
					},
					3: []map[string]any{
						{
							translate.CardinalFieldCustomerID:  "alice",
							translate.CardinalFieldCollectorID: "12345",
							translate.CardinalFieldTimestamp:   ts * 3,
							"item2":                            "value2",
						},
					},
				},
				"bob/12345": {
					2: []map[string]any{
						{
							translate.CardinalFieldCustomerID:  "bob",
							translate.CardinalFieldCollectorID: "12345",
							translate.CardinalFieldTimestamp:   ts * 2,
							"item3":                            "value3",
						},
					},
				},
			},
			map[string]map[int64]map[string]any{
				"alice/12345": {
					1: {
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   ts,
						"item1":                            "value1",
					},
					3: {
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   ts * 3,
						"item2":                            "value2",
					},
				},
				"bob/12345": {
					2: {
						translate.CardinalFieldCustomerID:  "bob",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   ts * 2,
						"item3":                            "value3",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := boxer.NewMemoryBuffer()
			box, err := boxer.NewBoxer(boxer.WithInterval(time.Second), boxer.WithBufferStorage(buffer))
			assert.NoError(t, err)
			e := &s3Exporter{
				boxer: box,
				tags:  make(map[string]map[int64]map[string]any),
			}
			result := e.partitionTableByCustomerIDAndInterval(tt.input, false)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.expectedTagmap, e.tags)
		})
	}
}

func TestPartitionByCustomerID(t *testing.T) {
	tests := []struct {
		name           string
		input          []map[string]any
		expected       map[string][]map[string]any
		expectedTagmap map[string]map[int64]map[string]any
	}{
		{
			"empty table",
			[]map[string]any{},
			map[string][]map[string]any{},
			map[string]map[int64]map[string]any{},
		},
		{
			"single entry",
			[]map[string]any{
				{
					translate.CardinalFieldCustomerID:  "alice",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   1000,
					"foo":                              "value",
				},
			},
			map[string][]map[string]any{
				"alice/12345": {
					{
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   1000,
						"foo":                              "value",
					},
				},
			},
			map[string]map[int64]map[string]any{
				"alice/12345": {
					1: {
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   1000,
						"foo":                              "value",
					},
				},
			},
		},
		{
			"multiple entries",
			[]map[string]any{
				{
					translate.CardinalFieldCustomerID:  "alice",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   1000,
					"item1":                            "value1",
				},
				{
					translate.CardinalFieldCustomerID:  "alice",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   3000,
					"item2":                            "value2",
				},
				{
					translate.CardinalFieldCustomerID:  "bob",
					translate.CardinalFieldCollectorID: "12345",
					translate.CardinalFieldTimestamp:   2000,
					"item3":                            "value3",
				},
			},
			map[string][]map[string]any{
				"alice/12345": {
					{
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   1000,
						"item1":                            "value1",
					},
					{
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   3000,
						"item2":                            "value2",
					},
				},
				"bob/12345": {
					{
						translate.CardinalFieldCustomerID:  "bob",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   2000,
						"item3":                            "value3",
					},
				},
			},
			map[string]map[int64]map[string]any{
				"alice/12345": {
					1: {
						translate.CardinalFieldCustomerID:  "alice",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   1000,
						"item1":                            "value1",
						"item2":                            "value2",
					},
				},
				"bob/12345": {
					1: {
						translate.CardinalFieldCustomerID:  "bob",
						translate.CardinalFieldCollectorID: "12345",
						translate.CardinalFieldTimestamp:   2000,
						"item3":                            "value3",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := boxer.NewMemoryBuffer()
			box, err := boxer.NewBoxer(boxer.WithInterval(time.Second), boxer.WithBufferStorage(buffer))
			assert.NoError(t, err)
			e := &s3Exporter{
				boxer: box,
				tags:  make(map[string]map[int64]map[string]any),
			}
			result := e.partitionTableByCustomerID(1, tt.input)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.expectedTagmap, e.tags)
		})
	}
}

func TestGetKey(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		input    map[string]any
		expected string
	}{
		{
			"config with CustomerKey",
			&Config{
				S3Uploader: S3UploaderConfig{
					CustomerKey: "custom_key",
				},
			},
			map[string]any{
				translate.CardinalFieldCustomerID:  "alice",
				translate.CardinalFieldCollectorID: "12345",
			},
			"custom_key",
		},
		{
			"config without CustomerKey",
			&Config{
				S3Uploader: S3UploaderConfig{
					CustomerKey: "",
				},
			},
			map[string]any{
				translate.CardinalFieldCustomerID:  "alice",
				translate.CardinalFieldCollectorID: "12345",
			},
			"alice/12345",
		},
		{
			"nil config",
			nil,
			map[string]any{
				translate.CardinalFieldCustomerID:  "alice",
				translate.CardinalFieldCollectorID: "12345",
			},
			"alice/12345",
		},
		{
			"missing customer_id and collector_id",
			nil,
			map[string]any{
				"foo": "value",
			},
			"_default/_default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &s3Exporter{
				config: tt.config,
			}
			result := e.getKey(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
