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
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type datadogExporter struct {
	config     *Config
	httpClient *http.Client
	apiKey     string
	endpoint   string

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	messagesReceived  metric.Int64Counter
	messagesSubmitted metric.Int64Counter
	commonAttributes  attribute.Set

	logger *zap.Logger
}

func newDatadogExporter(config *Config, params exporter.CreateSettings, ttype string) *datadogExporter {
	e := &datadogExporter{
		config:            config,
		telemetrySettings: params.TelemetrySettings,
		logger:            params.Logger,
	}

	e.commonAttributes = attribute.NewSet(
		attribute.String("exporterId", params.ID.Name()),
	)

	p := params.TelemetrySettings.MeterProvider.Meter("otelcol/chqdatadog")

	received, err := p.Int64Counter("chqdatadog.exporter."+ttype+".received",
		metric.WithDescription("The number of log messages received."))
	if err != nil {
		e.logger.Error("Failed to create metric", zap.Error(err))
	}
	e.messagesReceived = received

	submitted, err := p.Int64Counter("chqdatadog.exporter."+ttype+".submitted",
		metric.WithDescription("The number of log messages submitted to dd endpoint."))
	if err != nil {
		e.logger.Error("Failed to create metric", zap.Error(err))
	}
	e.messagesSubmitted = submitted

	return e
}

func (e *datadogExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *datadogExporter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient
	return nil
}
