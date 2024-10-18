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

package pitbullprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor/internal/metadata"
)

// NewFactory creates a factory for S3 exporter.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createSpansProcessor, metadata.TracesStability),
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
	)
}

const (
	defaultDropDecorationTags = true
	defaultMetricAggregation  = 10 * time.Second
	defaultStatisticsInterval = 1 * time.Minute
)

func createDefaultConfig() component.Config {
	return &Config{
		Statistics: StatisticsConfig{
			Phase:    "postsample",
			Interval: defaultStatisticsInterval,
			ClientConfig: confighttp.ClientConfig{
				Timeout: 5 * time.Second,
				Headers: map[string]configopaque.String{
					"User-Agent": "cardinalhq-otel-collector",
				},
				Compression: configcompression.TypeGzip,
			},
		},
		MetricAggregation: MetricAggregationConfig{
			Interval: defaultMetricAggregation,
		},
		DropDecorationAttributes: defaultDropDecorationTags,
	}
}

func createLogsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	e, err := newPitbull(cfg.(*Config), "logs", set, nil)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(
		ctx, set, cfg, nextConsumer,
		e.ConsumeLogs,
		processorhelper.WithStart(e.Start),
		processorhelper.WithShutdown(e.Shutdown),
		processorhelper.WithCapabilities(e.Capabilities()))
}

func createMetricsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Metrics) (processor.Metrics, error) {
	e, err := newPitbull(cfg.(*Config), "metrics", set, nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(
		ctx, set, cfg, nextConsumer,
		e.ConsumeMetrics,
		processorhelper.WithStart(e.Start),
		processorhelper.WithShutdown(e.Shutdown),
		processorhelper.WithCapabilities(e.Capabilities()))
}

func createSpansProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Traces) (processor.Traces, error) {
	e, err := newPitbull(cfg.(*Config), "traces", set, nil)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(
		ctx, set, cfg, nextConsumer,
		e.ConsumeTraces,
		processorhelper.WithStart(e.Start),
		processorhelper.WithShutdown(e.Shutdown),
		processorhelper.WithCapabilities(e.Capabilities()))
}
