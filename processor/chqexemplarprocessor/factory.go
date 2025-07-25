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

package chqexemplarprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/signalnames"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqexemplarprocessor/internal/metadata"
)

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
	defaultReportingInterval = 5 * time.Minute
	defaultExpiry            = defaultReportingInterval * 3
	defaultLRUCacheSize      = 10000
	defaultBatchSize         = 200
)

func createDefaultConfig() component.Config {
	return &Config{
		Reporting: ReportingConfig{
			Metrics: EnabledOption{
				Enabled:   true,
				Interval:  defaultReportingInterval,
				Expiry:    defaultExpiry,
				CacheSize: defaultLRUCacheSize,
				BatchSize: defaultBatchSize,
			},
			Logs: EnabledOption{
				Enabled:   true,
				Interval:  defaultReportingInterval,
				Expiry:    defaultExpiry,
				CacheSize: defaultLRUCacheSize,
				BatchSize: defaultBatchSize,
			},
			Traces: EnabledOption{
				Enabled:   true,
				Interval:  defaultReportingInterval,
				Expiry:    defaultExpiry,
				CacheSize: defaultLRUCacheSize,
				BatchSize: defaultBatchSize,
			},
		},
	}
}

func createLogsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	p, err := newProcessor(cfg.(*Config), signalnames.Logs, set)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(
		ctx, set, cfg, nextConsumer,
		p.ConsumeLogs,
		processorhelper.WithStart(p.Start),
		processorhelper.WithCapabilities(p.Capabilities()))
}

func createMetricsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Metrics) (processor.Metrics, error) {
	p, err := newProcessor(cfg.(*Config), signalnames.Metrics, set)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(
		ctx, set, cfg, nextConsumer,
		p.ConsumeMetrics,
		processorhelper.WithStart(p.Start),
		processorhelper.WithCapabilities(p.Capabilities()))
}

func createSpansProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Traces) (processor.Traces, error) {
	p, err := newProcessor(cfg.(*Config), signalnames.Traces, set)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(
		ctx, set, cfg, nextConsumer,
		p.ConsumeTraces,
		processorhelper.WithStart(p.Start),
		processorhelper.WithCapabilities(p.Capabilities()))
}
