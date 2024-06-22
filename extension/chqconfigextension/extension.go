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

package chqconfigextension

import (
	"context"
	"net/http"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension/internal/metadata"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/filereader"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
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
	configManager      sampler.ConfigManager
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
		httpClientSettings: cfg.Source.ClientConfig,
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

	var fr filereader.FileReader
	if chq.config.Source.scheme == "http" || chq.config.Source.scheme == "https" {
		fr = filereader.NewHTTPFileReader(chq.config.Source.Endpoint, chq.httpClient)
	} else {
		fr = filereader.NewLocalFileReader(chq.config.Source.Endpoint)
	}
	chq.configManager = sampler.NewConfigManagerImpl(chq.logger, chq.config.CheckInterval, fr)
	chq.logger.Info("Starting configuration manager", zap.String("check_interval", chq.config.CheckInterval.String()))
	go chq.configManager.Run()
	return nil
}

func (chq *CHQConfigExtension) Shutdown(context.Context) error {
	chq.logger.Info("Stopping configuration manager")
	chq.configManager.Stop()
	chq.logger.Info("Configuration manager stopped")
	return nil
}

func (chq *CHQConfigExtension) RegisterCallback(name string, cb sampler.ConfigUpdateCallbackFunc) int {
	return chq.configManager.RegisterCallback(name, cb)
}

func (chq *CHQConfigExtension) UnregisterCallback(id int) {
	chq.configManager.UnregisterCallback(id)
}
