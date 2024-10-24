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

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
)

func TestExtractMetricsFromLogs_MultipleLogsMatchingCondition(t *testing.T) {
	logger := zap.NewNop()

	extractorConfigs := []ottl.MetricExtractorConfig{
		{
			Route:       "route-1",
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
	metricsByRoute := e.extractMetricsFromLogs(context.Background(), logs)
	assert.NotNil(t, metricsByRoute)
	assert.Len(t, metricsByRoute, 1)
	// check if metricByRoute has an entry for route-1
	metrics, ok := metricsByRoute["route-1"]
	assert.True(t, ok)
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

func newLogsTestExtractor(logExtractors []*ottl.LogExtractor) *extractor {
	config := &Config{}
	ttype := "logs"
	set := processor.Settings{}

	e := &extractor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
	}
	e.logExtractors = ottl.ConvertToPointerArray(logExtractors)
	return e
}
