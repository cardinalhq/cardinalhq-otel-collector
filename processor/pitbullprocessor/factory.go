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

package pitbullprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper"
	"go.opentelemetry.io/collector/processor/xprocessor"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/signalnames"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor/internal/metadata"
)

// NewFactory returns a new factory for the Pitbull processor.
func NewFactory() xprocessor.Factory {
	return xprocessor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xprocessor.WithTraces(createTracesProcessor, metadata.TracesStability),
		xprocessor.WithLogs(createLogsProcessor, metadata.LogsStability),
		xprocessor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		xprocessor.WithProfiles(createProfilesProcessor, metadata.ProfilesStability),
	)
}

const (
	defaultMetricAggregation  = 10 * time.Second
	defaultStatisticsInterval = 1 * time.Minute
)

func createDefaultConfig() component.Config {
	return &Config{
		MetricAggregation: MetricAggregationConfig{
			Interval: defaultMetricAggregation,
		},
		TracesConfig: TracesConfig{
			EstimatorWindowSize: 30,
			EstimatorInterval:   10000,
		},
	}
}

func createLogsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	p, err := newPitbull(cfg.(*Config), signalnames.Logs, set)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(
		ctx, set, cfg, nextConsumer,
		p.ConsumeLogs,
		processorhelper.WithStart(p.Start),
		processorhelper.WithShutdown(p.Shutdown),
		processorhelper.WithCapabilities(p.Capabilities()))
}

func createMetricsProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Metrics) (processor.Metrics, error) {
	p, err := newPitbull(cfg.(*Config), signalnames.Metrics, set)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(
		ctx, set, cfg, nextConsumer,
		p.ConsumeMetrics,
		processorhelper.WithStart(p.Start),
		processorhelper.WithShutdown(p.Shutdown),
		processorhelper.WithCapabilities(p.Capabilities()))
}

func createTracesProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer consumer.Traces) (processor.Traces, error) {
	p, err := newPitbull(cfg.(*Config), signalnames.Traces, set)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(
		ctx, set, cfg, nextConsumer,
		p.ConsumeTraces,
		processorhelper.WithStart(p.Start),
		processorhelper.WithShutdown(p.Shutdown),
		processorhelper.WithCapabilities(p.Capabilities()))
}

func createProfilesProcessor(ctx context.Context, set processor.Settings, cfg component.Config, nextConsumer xconsumer.Profiles) (xprocessor.Profiles, error) {
	p, err := newPitbull(cfg.(*Config), signalnames.Profiles, set)
	if err != nil {
		return nil, err
	}
	return xprocessorhelper.NewProfiles(
		ctx, set, cfg, nextConsumer,
		p.ConsumeProfiles,
		xprocessorhelper.WithStart(p.Start),
		xprocessorhelper.WithShutdown(p.Shutdown),
		xprocessorhelper.WithCapabilities(p.Capabilities()))
}
