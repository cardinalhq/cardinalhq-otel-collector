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

package chqservicegraphexporter

import (
	"context"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqservicegraphexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.ClientConfig{
			Timeout: 5 * time.Second,
			Headers: configopaque.MapList{
				{Name: "User-Agent", Value: "cardinalhq-otel-collector"},
			},
			Compression: configcompression.TypeGzip,
		},
	}
}

func createMetricsExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Metrics, error) {
	cfg := config.(*Config)
	e := newServiceGraphExporter(cfg, params)
	exp, err := exporterhelper.NewMetrics(
		ctx, params, config,
		e.ConsumeMetrics,
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
	if err != nil {
		return nil, err
	}
	return exp, nil
}
