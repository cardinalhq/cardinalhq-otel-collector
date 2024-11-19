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

package extractmetricsprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/telemetry"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/extractmetricsprocessor/internal/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	embeddedmetric "go.opentelemetry.io/otel/metric/embedded"
	noopmetric "go.opentelemetry.io/otel/metric/noop"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/ottl"
)

func TestExtractMetricsFromLogs_MultipleLogsMatchingCondition(t *testing.T) {
	logger := zap.NewNop()

	extractorConfigs := []ottl.MetricExtractorConfig{
		{
			MetricName:  "test_metric",
			MetricUnit:  "ms",
			MetricType:  gaugeDoubleType,
			MetricValue: `body["sent_bytes"]`,
			Conditions:  []string{`attributes["log_type"] == "http"`},
			Dimensions: map[string]string{
				"statusCode": `attributes["status_code"]`,
			},
		},
	}

	configs, err := ottl.ParseLogExtractorConfigs(extractorConfigs, logger)
	assert.NoError(t, err)
	assert.Len(t, configs, 1)
	logExtractor := configs[0]
	assert.NotNil(t, logExtractor)
	assert.NotNil(t, logExtractor.Dimensions)
	assert.Len(t, logExtractor.Dimensions, 1)
	assert.NotNil(t, logExtractor.Conditions)
	assert.NotNil(t, logExtractor.MetricValue)

	// Create the logs dataset
	logs := plog.NewLogs()
	rLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := rLogs.ScopeLogs().AppendEmpty()
	logRecord1 := scopeLogs.LogRecords().AppendEmpty()
	logRecord1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord1.Body().SetEmptyMap()
	logRecord1.Body().Map().PutDouble("sent_bytes", 100)
	logRecord1.Attributes().PutStr("log_type", "http")
	logRecord1.Attributes().PutStr("status_code", "OK")

	logRecord2 := scopeLogs.LogRecords().AppendEmpty()
	logRecord2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Minute)))
	logRecord2.Body().SetEmptyMap()
	logRecord2.Body().Map().PutDouble("sent_bytes", 100)
	logRecord2.Attributes().PutStr("log_type", "http")
	logRecord2.Attributes().PutStr("status_code", "OK")

	// Extract metrics
	e := newLogsTestExtractor(configs)
	metrics := e.extractMetricsFromLogs(context.Background(), logs)
	assert.Len(t, metrics, 1)
	metric := metrics[0]
	assert.NotNil(t, metric)
	assert.Equal(t, "test_metric", metric.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	assert.Equal(t, "ms", metric.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Unit())
	assert.Equal(t, 2, metric.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().Len())
	datapoint0 := metric.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	assert.Equal(t, 100.0, datapoint0.DoubleValue())
	statusCode0, statusCode0Found := datapoint0.Attributes().Get("statusCode")
	assert.True(t, statusCode0Found)
	assert.Equal(t, "OK", statusCode0.Str())

	datapoint1 := metric.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(1)
	statusCode1, statusCode1Found := datapoint1.Attributes().Get("statusCode")
	assert.True(t, statusCode1Found)
	assert.Equal(t, "OK", statusCode1.Str())
	assert.Equal(t, 100.0, datapoint1.DoubleValue())
}

func TestExtractTimestampFromLogRecord(t *testing.T) {
	now := time.Now()
	nowTs := pcommon.NewTimestampFromTime(now)
	observedTs := pcommon.NewTimestampFromTime(now.Add(-time.Minute))

	tests := []struct {
		name     string
		setup    func() plog.LogRecord
		expected pcommon.Timestamp
	}{
		{
			name: "Timestamp is set",
			setup: func() plog.LogRecord {
				lr := plog.NewLogRecord()
				lr.SetTimestamp(nowTs)
				return lr
			},
			expected: nowTs,
		},
		{
			name: "ObservedTimestamp is set",
			setup: func() plog.LogRecord {
				lr := plog.NewLogRecord()
				lr.SetObservedTimestamp(observedTs)
				return lr
			},
			expected: observedTs,
		},
		{
			name: "Neither Timestamp nor ObservedTimestamp is set",
			setup: func() plog.LogRecord {
				return plog.NewLogRecord()
			},
			expected: pcommon.NewTimestampFromTime(time.Now()), // This will be close to the current time
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := tt.setup()
			actual := extractTimestampFromLogRecord(lr)
			if tt.name == "Neither Timestamp nor ObservedTimestamp is set" {
				assert.WithinDuration(t, now, actual.AsTime(), time.Second)
			} else {
				assert.Equal(t, tt.expected, actual)
			}
		})
	}
}

type mockMeterProvider struct {
	embeddedmetric.MeterProvider
}

type mockMeter struct {
	noopmetric.Meter
	name string
}

func (m mockMeterProvider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return mockMeter{name: name}
}

func newLogsTestExtractor(logExtractors []*ottl.LogExtractor) *extractor {
	config := &Config{}
	ttype := "logs"
	set := processor.Settings{}

	e := &extractor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            zap.NewNop(),
	}
	attrset := attribute.NewSet(
		attribute.String("processor", "test"),
		attribute.String("signal", "logs"),
	)
	set.MeterProvider = mockMeterProvider{}
	counter, _ := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"extract_metric_rules_evaluated",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of rules evaluated"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	e.rulesEvaluated = counter

	errorCounter, _ := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"errors",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of errors"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	histogram, _ := telemetry.NewDeferrableHistogram(metadata.Meter(set.TelemetrySettings),
		"ottl_rule_eval_time",
		[]metric.Int64HistogramOption{},
		[]metric.RecordOption{
			metric.WithAttributeSet(attrset),
		},
	)
	e.rulesEvaluated = counter
	e.ruleErrors = errorCounter
	e.ruleEvalTime = histogram

	e.logExtractors.Store(&logExtractors)
	return e
}
