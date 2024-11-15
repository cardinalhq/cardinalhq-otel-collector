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

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/ottl"
)

func TestExtractMetricsFromSpans_MultipleSpansMatchingCondition(t *testing.T) {
	logger := zap.NewNop()

	extractorConfigs := []ottl.MetricExtractorConfig{
		{
			MetricName:  "test_metric",
			MetricUnit:  "ms",
			MetricType:  gaugeDoubleType,
			MetricValue: `attributes["random_number"]`,
			Conditions:  []string{`attributes["span_type"] == "http"`},
			Dimensions: map[string]string{
				"statusCode": `attributes["status_code"]`,
			},
		},
	}

	configs, err := ottl.ParseSpanExtractorConfigs(extractorConfigs, logger)
	assert.NoError(t, err)
	assert.Len(t, configs, 1)
	spanExtractor := configs[0]
	assert.NotNil(t, spanExtractor)
	assert.NotNil(t, spanExtractor.Dimensions)
	assert.Len(t, spanExtractor.Dimensions, 1)
	assert.NotNil(t, spanExtractor.Conditions)
	assert.NotNil(t, spanExtractor.MetricValue)

	traces := ptrace.NewTraces()
	rSpans := traces.ResourceSpans().AppendEmpty()
	scopeLogs := rSpans.ScopeSpans().AppendEmpty()
	spanRecord1 := scopeLogs.Spans().AppendEmpty()
	spanRecord1.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	spanRecord1.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	spanRecord1.SetKind(ptrace.SpanKindClient)
	spanRecord1.Attributes().PutStr("span_type", "http")
	spanRecord1.Attributes().PutStr("status_code", "OK")
	spanRecord1.Attributes().PutDouble("random_number", 100)

	spanRecord2 := scopeLogs.Spans().AppendEmpty()
	spanRecord2.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	spanRecord2.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	spanRecord2.SetKind(ptrace.SpanKindClient)
	spanRecord2.Attributes().PutStr("span_type", "http")
	spanRecord2.Attributes().PutStr("status_code", "OK")
	spanRecord2.Attributes().PutDouble("random_number", 100)

	// Extract metrics
	e := newSpansTestExtractor(configs)
	metrics := e.extractMetricsFromSpans(context.Background(), traces)
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

func newSpansTestExtractor(logExtractors []*ottl.SpanExtractor) *extractor {
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
	counter, err := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"extract_metric_rules_evaluated",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of rules evaluated"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if err != nil {
		return nil
	}

	e.rulesEvaluated = counter
	e.spanExtractors.Store(&logExtractors)
	return e
}
