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

package chqstatsexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqstatsexporter/internal/metadata"
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
		Interval: 5 * time.Minute,
		ClientConfig: confighttp.ClientConfig{
			Timeout:  5 * time.Second,
			Endpoint: "https://api.cardinalhq.com",
			Headers: map[string]configopaque.String{
				"User-Agent": "cardinalhq-otel-collector",
			},
			Compression: configcompression.TypeGzip,
		},
	}
}

func createLogsExporter(ctx context.Context,
	params exporter.Settings,
	config component.Config) (exporter.Logs, error) {
	e := newStatsExporter(config.(*Config), params)
	return exporterhelper.NewLogsExporter(
		ctx, params, config,
		e.ConsumeLogs,
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
}

func createMetricsExporter(ctx context.Context,
	params exporter.Settings,
	config component.Config) (exporter.Metrics, error) {
	e := newStatsExporter(config.(*Config), params)
	return exporterhelper.NewMetricsExporter(
		ctx, params, config,
		e.ConsumeMetrics,
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
}

func createTracesExporter(ctx context.Context,
	params exporter.Settings,
	config component.Config) (exporter.Traces, error) {
	e := newStatsExporter(config.(*Config), params)
	return exporterhelper.NewTracesExporter(
		ctx, params, config,
		e.ConsumeTraces,
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
}
