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

package chqenforcerprocessor

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

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqenforcerprocessor/internal/metadata"
)

// NewFactory creates a factory for S3 exporter.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
	)
}

const (
	defaultDropDecorationTags = true
)

func createDefaultConfig() component.Config {
	return &Config{
		Statistics: StatisticsConfig{
			Phase:    "postsample",
			Interval: 5 * time.Minute,
			ClientConfig: confighttp.ClientConfig{
				Timeout:  5 * time.Second,
				Endpoint: "https://api.cardinalhq.com",
				Headers: map[string]configopaque.String{
					"User-Agent": "cardinalhq-otel-collector",
				},
				Compression: configcompression.TypeGzip,
			},
		},
		MetricAggregation: MetricAggregationConfig{
			Interval: 10 * time.Second,
		},
		DropDecorationAttributes: defaultDropDecorationTags,
	}
}

func createLogsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	e := newCHQEnforcer(cfg.(*Config), "logs", set, nil)
	return processorhelper.NewLogsProcessor(
		ctx, set, cfg, nextConsumer,
		e.ConsumeLogs,
		processorhelper.WithStart(e.Start),
		processorhelper.WithShutdown(e.Shutdown),
		processorhelper.WithCapabilities(e.Capabilities()))
}

func createMetricsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Metrics) (processor.Metrics, error) {
	e := newCHQEnforcer(cfg.(*Config), "metrics", set, nextConsumer)
	return processorhelper.NewMetricsProcessor(
		ctx, set, cfg, nextConsumer,
		e.ConsumeMetrics,
		processorhelper.WithStart(e.Start),
		processorhelper.WithShutdown(e.Shutdown),
		processorhelper.WithCapabilities(e.Capabilities()))
}

func createTracesProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Traces) (processor.Traces, error) {
	e := newCHQEnforcer(cfg.(*Config), "traces", set, nil)
	return processorhelper.NewTracesProcessor(
		ctx, set, cfg, nextConsumer,
		e.ConsumeTraces,
		processorhelper.WithStart(e.Start),
		processorhelper.WithShutdown(e.Shutdown),
		processorhelper.WithCapabilities(e.Capabilities()))
}
