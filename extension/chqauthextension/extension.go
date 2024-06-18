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
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension/internal/metadata"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/auth"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	apiKeyHeader = "x-cardinalhq-api-key"
)

var (
	errNoAuthHeader = errors.New("no authentication header found")
	errDenied       = errors.New("authentication denied")
)

type chqAuth struct {
	config     *Config
	httpClient *http.Client

	lookupCache map[string]*authData
	cacheLock   sync.Mutex

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	authCacheLookups metric.Int64Counter
	authCacheAdds    metric.Int64Counter

	logger *zap.Logger
}

func (chq *chqAuth) setupTelemetry(params extension.Settings) error {
	m, err := metadata.Meter(params.TelemetrySettings).Int64Counter("auth_cache_lookups")
	if err != nil {
		return err
	}
	chq.authCacheLookups = m

	m, err = metadata.Meter(params.TelemetrySettings).Int64Counter("auth_cache_adds")
	if err != nil {
		return err
	}
	chq.authCacheAdds = m

	return nil
}

func newServerAuthExtension(cfg *Config, params extension.Settings) (auth.Server, error) {
	chq := chqAuth{
		config:             cfg,
		httpClientSettings: cfg.ServerAuth.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		logger:             params.Logger,
		lookupCache:        make(map[string]*authData),
	}
	if err := chq.setupTelemetry(params); err != nil {
		return nil, err
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

	authData, err := chq.authenticateAPIKey(ctx, auth)
	if err != nil {
		return ctx, err
	}

	cl := client.FromContext(ctx)
	cl.Auth = authData
	return client.NewContext(ctx, cl), nil
}

type validateResponse struct {
	Valid bool   `json:"valid"`
	ID    string `json:"id"`
	Name  string `json:"name"`
}

func (chq *chqAuth) getcache(apiKey string) *authData {
	chq.cacheLock.Lock()
	defer chq.cacheLock.Unlock()
	ad, ok := chq.lookupCache[apiKey]
	if !ok {
		attrs := metric.WithAttributes(attribute.String("cache", "miss"))
		chq.authCacheLookups.Add(context.Background(), 1, attrs)
		return nil
	}
	if ad.expiry.Before(time.Now()) {
		attrs := metric.WithAttributes(attribute.String("cache", "expired"))
		chq.authCacheLookups.Add(context.Background(), 1, attrs)
		delete(chq.lookupCache, apiKey)
		return nil
	}
	attrs := metric.WithAttributes(attribute.String("cache", "hit"))
	chq.authCacheLookups.Add(context.Background(), 1, attrs)
	return ad
}

func (chq *chqAuth) setcache(ad *authData) {
	chq.authCacheAdds.Add(context.Background(), 1)
	chq.cacheLock.Lock()
	defer chq.cacheLock.Unlock()
	chq.lookupCache[ad.apiKey] = ad
}

func (chq *chqAuth) authenticateAPIKey(ctx context.Context, apiKey string) (*authData, error) {
	ad := chq.getcache(apiKey)
	if ad != nil {
		if !ad.valid {
			return nil, errDenied
		}
		return ad, nil
	}

	ad, err := chq.callValidateAPI(ctx, apiKey)
	if err != nil {
		if errors.Is(err, errDenied) {
			ad = &authData{
				apiKey: apiKey,
				valid:  false,
				expiry: time.Now().Add(chq.config.ServerAuth.CacheTTLInvalid),
			}
			chq.setcache(ad)
		}
		return nil, err
	}
	ad.expiry = time.Now().Add(chq.config.ServerAuth.CacheTTLValid)
	chq.setcache(ad)
	return ad, nil
}

func (chq *chqAuth) callValidateAPI(ctx context.Context, apiKey string) (*authData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, chq.config.ServerAuth.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(apiKeyHeader, apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := chq.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errDenied
	}

	var validateResp validateResponse
	if err := json.NewDecoder(resp.Body).Decode(&validateResp); err != nil {
		return nil, err
	}

	return &authData{
		apiKey:     apiKey,
		clientID:   validateResp.ID,
		clientName: validateResp.Name,
		valid:      validateResp.Valid,
	}, nil
}

func getAuthHeader(h map[string][]string) string {
	const (
		headerKey = apiKeyHeader
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
	apiKey     string
	clientID   string
	clientName string
	valid      bool
	expiry     time.Time
}

func (a *authData) GetAttribute(name string) any {
	switch name {
	case "api_key":
		return a.apiKey
	case "client_id":
		return a.clientID
	case "client_name":
		return a.clientName
	case "valid":
		return a.valid
	default:
		return nil
	}
}

func (a *authData) GetAttributeNames() []string {
	return []string{"api_key", "client_id", "client_name", "valid"}
}
