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

package chqkubeeventsexporter

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type kubeEventsExporter struct {
	config             *Config
	httpClient         *http.Client
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	exportedEvents     metric.Int64Counter
	logger             *zap.Logger

	exportEvents func(kubeEvent []KubernetesEvent)
}

func newKubeEventsExporter(config *Config, params exporter.Settings) *kubeEventsExporter {
	e := &kubeEventsExporter{
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		logger:             params.Logger,
	}
	e.exportEvents = e.sendAsync

	p := params.TelemetrySettings.MeterProvider.Meter("otelcol/chqkubeevents")

	received, err := p.Int64Counter("exported_events",
		metric.WithDescription("The number of events exported"))
	if err != nil {
		e.logger.Error("Failed to create metric", zap.Error(err))
	}
	e.exportedEvents = received

	return e
}

func (e *kubeEventsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *kubeEventsExporter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient
	return nil
}
