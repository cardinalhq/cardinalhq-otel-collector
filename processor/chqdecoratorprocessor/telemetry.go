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

	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/metadata"

	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type trigger int

const (
	triggerMetricDataPointsDropped trigger = iota
	triggerLogsDropped
	triggerSpansDropped
)

type decoratorProcessorTelemetry struct {
	exportCtx context.Context

	processorAttr []attribute.KeyValue

	datapointsFiltered metric.Int64Counter
	logsFiltered       metric.Int64Counter
	spansFiltered      metric.Int64Counter
}

func newDecoratorProcessorTelemetry(set processor.CreateSettings) (*decoratorProcessorTelemetry, error) {
	processorID := set.ID.String()

	dpt := &decoratorProcessorTelemetry{
		processorAttr: []attribute.KeyValue{attribute.String(metadata.Type.String(), processorID)},
		exportCtx:     context.Background(),
	}

	counter, err := metadata.Meter(set.TelemetrySettings).Int64Counter(
		processorhelper.BuildCustomMetricName(metadata.Type.String(), "datapoints.filtered"),
		metric.WithDescription("The total number of datapoints filtered by the processor"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	dpt.datapointsFiltered = counter

	counter, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		processorhelper.BuildCustomMetricName(metadata.Type.String(), "logs.filtered"),
		metric.WithDescription("The total number of logs filtered by the processor"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	dpt.logsFiltered = counter

	counter, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		processorhelper.BuildCustomMetricName(metadata.Type.String(), "spans.filtered"),
		metric.WithDescription("The total number of spans filtered by the processor"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	dpt.spansFiltered = counter

	return dpt, nil
}

func (dpt *decoratorProcessorTelemetry) record(trigger trigger, dropped int64) {
	var triggerMeasure metric.Int64Counter
	switch trigger {
	case triggerMetricDataPointsDropped:
		triggerMeasure = dpt.datapointsFiltered
	case triggerLogsDropped:
		triggerMeasure = dpt.logsFiltered
	case triggerSpansDropped:
		triggerMeasure = dpt.spansFiltered
	}

	triggerMeasure.Add(dpt.exportCtx, dropped, metric.WithAttributes(dpt.processorAttr...))
}