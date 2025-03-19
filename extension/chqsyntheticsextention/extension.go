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

package chqsyntheticsextention

import (
	"context"
	"errors"
	"net/http"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

type CHQSyntheticsExtension struct {
	id                 component.ID
	config             *Config
	httpClient         *http.Client
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	logger             *zap.Logger

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

	// Ruchir TODO: add this type to the oteltools package
	tenants syncmap.SyncMap[string, *ottl.SyntheticPollings]
}

func (chq *CHQSyntheticsExtension) setupTelemetry(params extension.Settings) error {
	// TODO: add some sort of intenral metrics here, likely histograms for the queries we make

	return nil
}

func newExtension(cfg *Config, set extension.Settings) (*CHQSyntheticsExtension, error) {
	chq := CHQSyntheticsExtension{
		id:                 set.ID,
		config:             cfg,
		httpClientSettings: cfg.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
	}
	if err := chq.setupTelemetry(set); err != nil {
		return nil, err
	}
	return &chq, nil
}

func (e *CHQSyntheticsExtension) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

	ext, found := host.GetExtensions()[*e.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	e.configExtension = cext
	e.configCallbackID = e.configExtension.RegisterCallback(e.id.String(), e.configUpdateCallback)

	return nil
}

func (e *CHQSyntheticsExtension) Shutdown(context.Context) error {
	e.configExtension.UnregisterCallback(e.configCallbackID)
	return nil
}

func (e *CHQSyntheticsExtension) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	e.logger.Info("Configuration updated for extension instance", zap.String("instance", e.id.Name()))
	for cid, tenant := range sc.Configs {
		tc := tenant.SyntheticPollings[e.id.Name()]
		if tc == nil {
			e.tenants.Delete(cid)
			continue
		}
		e.tenants.Store(cid, tc)
	}

	// Ruchir TODO: if you need to rebuild some internal state, here is where you would want to do that.
	// Perhaps also when removing tenants as well.
	// syncmap also has some other methods that can do things like replace and return the
	// old value, etc.
}
