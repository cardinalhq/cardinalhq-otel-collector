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
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension/internal/metadata"
)

// NewFactory creates a factory for the static bearer token Authenticator extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.ClientConfig{
			Timeout: time.Duration(5 * time.Second),
		},
		CheckInterval: time.Duration(5 * time.Minute),
		Endpoint:      "https://config.cardinalhq.io/api/v1/config",
	}
}

func createExtension(_ context.Context, params extension.Settings, cfg component.Config) (extension.Extension, error) {
	return newConfigExtension(cfg.(*Config), params)
}
