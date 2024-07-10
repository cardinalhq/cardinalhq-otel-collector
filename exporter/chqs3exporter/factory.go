// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/metadata"
)

// NewFactory creates a factory for S3 exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		IDSource: "env",
		S3Uploader: S3UploaderConfig{
			Region:      "us-east-1",
			S3Partition: "minute",
		},
		Buffering: BufferingConfig{
			Type: bufferTypeMemory,
		},
	}
}

func createLogsExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Logs, error) {
	exp, err := newS3Exporter(config.(*Config), params, logFilePrefix)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogsExporter(ctx, params,
		config,
		exp.ConsumeLogs,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithCapabilities(capabilities()))
}

func createMetricsExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Metrics, error) {
	exp, err := newS3Exporter(config.(*Config), params, metricFilePrefix)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetricsExporter(ctx, params,
		config,
		exp.ConsumeMetrics,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithCapabilities(capabilities()))
}

func createTracesExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Traces, error) {
	exp, err := newS3Exporter(config.(*Config), params, tracesFilePrefix)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTracesExporter(ctx,
		params,
		config,
		exp.ConsumeTraces,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithCapabilities(capabilities()))
}

func capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
