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

package chqtagcacheextension

import (
	"context"
	"net/http"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type CHQConfigExtension struct {
	config             *Config
	httpClient         *http.Client
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	fetches            metric.Int64Counter
	logger             *zap.Logger
}

func (chq *CHQConfigExtension) setupTelemetry(params extension.Settings) error {
	m, err := metadata.Meter(params.TelemetrySettings).Int64Counter("fetches")
	if err != nil {
		return err
	}
	chq.fetches = m

	return nil
}

func newConfigExtension(cfg *Config, params extension.Settings) (*CHQConfigExtension, error) {
	chq := CHQConfigExtension{
		config:             cfg,
		httpClientSettings: cfg.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		logger:             params.Logger,
	}
	if err := chq.setupTelemetry(params); err != nil {
		return nil, err
	}
	return &chq, nil
}

func (chq *CHQConfigExtension) Start(_ context.Context, host component.Host) error {
	httpClient, err := chq.httpClientSettings.ToClient(context.Background(), host, chq.telemetrySettings)
	if err != nil {
		return err
	}
	chq.httpClient = httpClient

	return nil
}

func (chq *CHQConfigExtension) Shutdown(context.Context) error {
	return nil
}
