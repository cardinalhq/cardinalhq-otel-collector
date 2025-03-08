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

package chqauthextension

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension/internal/metadata"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(_ context.Context, params extension.Settings, cfg component.Config) (extension.Extension, error) {
	c := cfg.(*Config)

	if c.ClientAuth == nil && c.ServerAuth == nil {
		return nil, errNoAuthConfig
	}
	if c.ClientAuth != nil && c.ServerAuth != nil {
		return nil, errDuplicateAuthConfig
	}

	if c.ClientAuth != nil {
		if c.ClientAuth.APIKey == "" {
			return nil, errNoClientAPIKey
		}
		return newClientAuthExtension(c, params)
	}

	if c.ServerAuth.Endpoint == "" {
		return nil, errServerAuthEndpoint
	}
	return newServerAuthExtension(c, params)
}
