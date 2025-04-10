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

package chqk8sentitygraphexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/metadata"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

const (
	defaultReportingInterval = 30 * time.Second
	defaultTimeout           = 15 * time.Second
)

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.ClientConfig{
			Timeout: defaultTimeout,
		},
		Reporting: ReportingConfig{
			Interval: defaultReportingInterval,
		},
	}
}

func createLogsProcessor(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	e, err := newEntityGraphExporter(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx, set, cfg,
		e.ConsumeLogs,
		exporterhelper.WithStart(e.Start),
		exporterhelper.WithCapabilities(e.Capabilities()))
}
