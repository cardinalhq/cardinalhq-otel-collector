// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chqs3exporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
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
		S3Uploader: S3UploaderConfig{
			Region:      "us-east-1",
			S3Partition: "minute",
		},
	}
}

func createLogsExporter(ctx context.Context,
	params exporter.Settings,
	config component.Config) (exporter.Logs, error) {

	s3Exporter := newS3Exporter(config.(*Config), params)

	return exporterhelper.NewLogsExporter(ctx, params,
		config,
		s3Exporter.ConsumeLogs,
		exporterhelper.WithShutdown(s3Exporter.Shutdown),
		exporterhelper.WithCapabilities(s3Exporter.Capabilities()))
}

func createMetricsExporter(ctx context.Context,
	params exporter.Settings,
	config component.Config) (exporter.Metrics, error) {

	s3Exporter := newS3Exporter(config.(*Config), params)

	return exporterhelper.NewMetricsExporter(ctx, params,
		config,
		s3Exporter.ConsumeMetrics,
		exporterhelper.WithShutdown(s3Exporter.Shutdown),
		exporterhelper.WithCapabilities(s3Exporter.Capabilities()))
}

func createTracesExporter(ctx context.Context,
	params exporter.Settings,
	config component.Config) (exporter.Traces, error) {

	s3Exporter := newS3Exporter(config.(*Config), params)

	return exporterhelper.NewTracesExporter(ctx,
		params,
		config,
		s3Exporter.ConsumeTraces,
		exporterhelper.WithShutdown(s3Exporter.Shutdown),
		exporterhelper.WithCapabilities(s3Exporter.Capabilities()))
}
