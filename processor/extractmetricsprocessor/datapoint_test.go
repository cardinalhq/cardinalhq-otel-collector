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

package extractmetricsprocessor

import (
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestUpdateDatapointSum_NewDatapoint(t *testing.T) {
	sbuilder := signalbuilder.NewMetricScopeBuilder(pmetric.NewScopeMetrics())
	builder, err := sbuilder.Metric("test", "dogs", pmetric.MetricTypeSum)
	require.NoError(t, err)
	val := 10.0
	attrs := pcommon.NewMap()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	updateDatapointSum(builder, val, attrs, timestamp)

	dp, _, _ := builder.Datapoint(attrs, timestamp)
	assert.Equal(t, val, dp.DoubleValue())
}

func TestUpdateDatapointSum_ExistingDatapoint(t *testing.T) {
	sbuilder := signalbuilder.NewMetricScopeBuilder(pmetric.NewScopeMetrics())
	builder, err := sbuilder.Metric("test", "dogs", pmetric.MetricTypeSum)
	require.NoError(t, err)
	val1 := 10.0
	val2 := 5.0
	attrs := pcommon.NewMap()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	updateDatapointSum(builder, val1, attrs, timestamp)
	updateDatapointSum(builder, val2, attrs, timestamp)

	dp, _, _ := builder.Datapoint(attrs, timestamp)
	assert.Equal(t, val1+val2, dp.DoubleValue())
}

func TestUpdateDatapointGauge_NewDatapoint(t *testing.T) {
	sbuilder := signalbuilder.NewMetricScopeBuilder(pmetric.NewScopeMetrics())
	builder, err := sbuilder.Metric("test", "s", pmetric.MetricTypeGauge)
	require.NoError(t, err)
	val := 10.0
	attrs := pcommon.NewMap()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	updateDatapointGauge(builder, val, attrs, timestamp)

	dp, _, _ := builder.Datapoint(attrs, timestamp)
	assert.Equal(t, val, dp.DoubleValue())
}

func TestUpdateDatapointGauge_ExistingDatapointLowerValue(t *testing.T) {
	sbuilder := signalbuilder.NewMetricScopeBuilder(pmetric.NewScopeMetrics())
	builder, err := sbuilder.Metric("test", "s", pmetric.MetricTypeGauge)
	require.NoError(t, err)
	val1 := 10.0
	val2 := 5.0
	attrs := pcommon.NewMap()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	updateDatapointGauge(builder, val1, attrs, timestamp)
	updateDatapointGauge(builder, val2, attrs, timestamp)

	dp, _, _ := builder.Datapoint(attrs, timestamp)
	assert.Equal(t, val1, dp.DoubleValue())
}

func TestUpdateDatapointGauge_ExistingDatapointHigherValue(t *testing.T) {
	sbuilder := signalbuilder.NewMetricScopeBuilder(pmetric.NewScopeMetrics())
	builder, err := sbuilder.Metric("test", "s", pmetric.MetricTypeGauge)
	require.NoError(t, err)
	val1 := 5.0
	val2 := 10.0
	attrs := pcommon.NewMap()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	updateDatapointGauge(builder, val1, attrs, timestamp)
	updateDatapointGauge(builder, val2, attrs, timestamp)

	dp, _, _ := builder.Datapoint(attrs, timestamp)
	assert.Equal(t, val2, dp.DoubleValue())
}
