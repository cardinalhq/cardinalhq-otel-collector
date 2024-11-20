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

package chqdatadogexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqdatadogexporter/internal/metadata"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

const defaultClientTimeout = 5 * time.Second
const defaultEndpoint = "https://intake.cardinalhq.io"
const userAgent = "cardinalhq-otel-collector-chqdatadogexporter"

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutConfig: exporterhelper.NewDefaultTimeoutConfig(),
		RetryConfig:   configretry.NewDefaultBackOffConfig(),
		QueueConfig:   exporterhelper.NewDefaultQueueConfig(),
		Metrics: MetricsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:  defaultClientTimeout,
				Endpoint: defaultEndpoint,
				Headers: map[string]configopaque.String{
					"User-Agent": userAgent,
				},
				Compression: configcompression.TypeGzip,
			},
		},
		Logs: LogsConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:  defaultClientTimeout,
				Endpoint: defaultEndpoint,
				Headers: map[string]configopaque.String{
					"User-Agent": userAgent,
				},
				Compression: configcompression.TypeGzip,
			},
		},
		Traces: TracesConfig{
			ClientConfig: confighttp.ClientConfig{
				Timeout:  defaultClientTimeout,
				Endpoint: defaultEndpoint,
				Headers: map[string]configopaque.String{
					"User-Agent": userAgent,
				},
				Compression: configcompression.TypeGzip,
			},
		},
	}
}

func createLogsExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Logs, error) {
	cfg := config.(*Config)
	e := newDatadogExporter(cfg, params, "logs")
	exp, err := exporterhelper.NewLogs(
		ctx, params, config,
		e.ConsumeLogs,
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(cfg.RetryConfig),
		exporterhelper.WithQueue(cfg.QueueConfig),
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
	if err != nil {
		return nil, err
	}
	e.apiKey = string(e.config.Logs.APIKey)
	e.endpoint = e.config.Logs.Endpoint
	return exp, nil
}

func createMetricsExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Metrics, error) {
	cfg := config.(*Config)
	e := newDatadogExporter(cfg, params, "metrics")
	exp, err := exporterhelper.NewMetrics(
		ctx, params, config,
		e.ConsumeMetrics,
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(cfg.RetryConfig),
		exporterhelper.WithQueue(cfg.QueueConfig),
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
	if err != nil {
		return nil, err
	}
	e.apiKey = string(e.config.Metrics.APIKey)
	e.endpoint = e.config.Metrics.Endpoint
	return exp, nil
}

func createTracesExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Traces, error) {
	cfg := config.(*Config)
	e := newDatadogExporter(cfg, params, "traces")
	exp, err := exporterhelper.NewTraces(
		ctx, params, config,
		e.ConsumeTraces,
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(cfg.RetryConfig),
		exporterhelper.WithQueue(cfg.QueueConfig),
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
	if err != nil {
		return nil, err
	}
	e.apiKey = string(e.config.Traces.APIKey)
	e.endpoint = e.config.Traces.Endpoint
	return exp, nil
}
