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

package chqauthextension

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/auth"
	"go.uber.org/zap"
)

var (
	errNoAuthHeader = errors.New("no authentication header found")
)

type chqAuth struct {
	config     *Config
	httpClient *http.Client

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	logger *zap.Logger
}

func newServerAuthExtension(cfg *Config, params extension.CreateSettings) (auth.Server, error) {
	chq := chqAuth{
		config:             cfg,
		httpClientSettings: cfg.ServerAuth.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		logger:             params.Logger,
	}
	return auth.NewServer(
		auth.WithServerStart(chq.serverStart),
		auth.WithServerAuthenticate(chq.authenticate),
	), nil
}

func (chq *chqAuth) serverStart(_ context.Context, _ component.Host) error {
	httpClient, err := chq.httpClientSettings.ToClient(context.Background(), nil, chq.telemetrySettings)
	if err != nil {
		return err
	}
	chq.httpClient = httpClient
	return nil
}

func (chq *chqAuth) authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	auth := getAuthHeader(headers)
	if auth == "" {
		return ctx, errNoAuthHeader
	}

	cl := client.FromContext(ctx)
	cl.Auth = &authData{apiKey: auth}
	return client.NewContext(ctx, cl), nil
}

func getAuthHeader(h map[string][]string) string {
	const (
		headerKey = "x-cardinalhq-api-key"
	)
	for k, v := range h {
		if strings.EqualFold(k, headerKey) {
			return v[0]
		}
	}
	return ""
}

var _ client.AuthData = (*authData)(nil)

type authData struct {
	apiKey string
}

func (a *authData) GetAttribute(name string) any {
	switch name {
	case "api_key":
		return a.apiKey
	default:
		return nil
	}
}

func (a *authData) GetAttributeNames() []string {
	return []string{"api_key"}
}
