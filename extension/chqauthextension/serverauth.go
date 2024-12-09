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
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/auth"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension/internal/metadata"
)

type chqServerAuth struct {
	config            *Config
	logger            *zap.Logger
	telemetrySettings component.TelemetrySettings

	// for server auth
	httpClient         *http.Client
	lookupCache        map[string]*authData
	cacheLock          sync.Mutex
	httpClientSettings confighttp.ClientConfig
	authCacheLookups   metric.Int64Counter
	authCacheAdds      metric.Int64Counter
}

var (
	errNoAuthHeader = errors.New("no authentication header found")
	errDenied       = errors.New("authentication denied")
)

func (chq *chqServerAuth) setupServerTelemetry(params extension.Settings) error {
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
	chq := chqServerAuth{
		config:             cfg,
		httpClientSettings: cfg.ServerAuth.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		logger:             params.Logger,
		lookupCache:        make(map[string]*authData),
	}
	if err := chq.setupServerTelemetry(params); err != nil {
		return nil, err
	}
	return auth.NewServer(
		auth.WithServerStart(chq.serverStart),
		auth.WithServerAuthenticate(chq.serverAuthenticate),
	), nil
}

func (chq *chqServerAuth) serverStart(_ context.Context, _ component.Host) error {
	httpClient, err := chq.httpClientSettings.ToClient(context.Background(), nil, chq.telemetrySettings)
	if err != nil {
		return err
	}
	chq.httpClient = httpClient
	return nil
}

func (chq *chqServerAuth) serverAuthenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	auth := getAuthHeader(headers)
	if auth == "" {
		return ctx, errNoAuthHeader
	}
	collectorID := getCollectorFromHeaders(headers)

	envkeys := getEnvFromHeaders(headers)

	authData, err := chq.authenticateAPIKey(ctx, auth, collectorID)
	if err != nil {
		return ctx, err
	}
	authData.environment = envkeys

	cl := client.FromContext(ctx)
	cl.Auth = authData
	return client.NewContext(ctx, cl), nil
}

type validateResponse struct {
	CustomerID   string `json:"customer_id"`
	CustomerName string `json:"customer_name"`
	CollectorID  string `json:"collector_id"`
	Valid        bool   `json:"valid"`
}

func (chq *chqServerAuth) getcache(cacheKey string) *authData {
	chq.cacheLock.Lock()
	defer chq.cacheLock.Unlock()
	ad, ok := chq.lookupCache[cacheKey]
	if !ok {
		attrs := metric.WithAttributes(attribute.String("cache", "miss"))
		chq.authCacheLookups.Add(context.Background(), 1, attrs)
		return nil
	}
	if ad.expiry.Before(time.Now()) {
		attrs := metric.WithAttributes(attribute.String("cache", "expired"))
		chq.authCacheLookups.Add(context.Background(), 1, attrs)
		delete(chq.lookupCache, cacheKey)
		return nil
	}
	attrs := metric.WithAttributes(attribute.String("cache", "hit"))
	chq.authCacheLookups.Add(context.Background(), 1, attrs)
	return ad
}

func getCacheKey(apiKey, collectorID string) string {
	return apiKey + ":" + collectorID
}

func (chq *chqServerAuth) setcache(ad *authData) {
	chq.authCacheAdds.Add(context.Background(), 1)
	chq.cacheLock.Lock()
	defer chq.cacheLock.Unlock()
	chq.lookupCache[getCacheKey(ad.apiKey, ad.collectorID)] = ad
}

func (chq *chqServerAuth) authenticateAPIKey(ctx context.Context, apiKey, collectorID string) (*authData, error) {
	ad := chq.getcache(getCacheKey(apiKey, collectorID))
	if ad != nil {
		if !ad.valid {
			chq.logger.Info("cached auth denied", zap.Any("authData", ad))
			return nil, errDenied
		}
		chq.logger.Info("cached auth granted", zap.Any("authData", ad))
		return ad, nil
	}

	ad, err := chq.callValidateAPI(ctx, apiKey, collectorID)
	if err != nil {
		if errors.Is(err, errDenied) {
			ad = &authData{
				apiKey:      apiKey,
				collectorID: collectorID,
				valid:       false,
				expiry:      time.Now().Add(chq.config.ServerAuth.CacheTTLInvalid),
			}
			chq.setcache(ad)
			chq.logger.Info("auth denied", zap.Any("authData", ad))
		}
		return nil, err
	}
	ad.expiry = time.Now().Add(chq.config.ServerAuth.CacheTTLValid)
	chq.setcache(ad)
	chq.logger.Info("auth granted", zap.Any("authData", ad))
	return ad, nil
}

func (chq *chqServerAuth) callValidateAPI(ctx context.Context, apiKey, collectorID string) (*authData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, chq.config.ServerAuth.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(apiKeyHeader, apiKey)
	if collectorID != "" {
		req.Header.Set(collectorIDHeader, collectorID)
	}
	chq.logger.Info("collectorID", zap.String("collectorID", collectorID))
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

	chq.logger.Info("authResponse", zap.Any("validateResp", validateResp))

	return &authData{
		apiKey:       apiKey,
		customerID:   validateResp.CustomerID,
		customerName: validateResp.CustomerName,
		collectorID:  validateResp.CollectorID,
		valid:        validateResp.Valid,
	}, nil
}

func getAuthHeader(h map[string][]string) string {
	for _, key := range apikey_headers {
		for k, v := range h {
			if strings.EqualFold(k, key) {
				return v[0]
			}
		}
	}
	return ""
}

func getCollectorFromHeaders(h map[string][]string) string {
	for k, v := range h {
		slog.Info("header", slog.String("key", k), slog.Any("value", v))
		if strings.EqualFold(k, collectorIDHeader) {
			return v[0]
		}
	}
	return ""
}

func getEnvFromHeaders(h map[string][]string) map[string]string {
	for k, v := range h {
		if strings.EqualFold(k, envKeyHeader) {
			return parseEnv(v[0])
		}
	}
	return map[string]string{}
}

func parseEnv(env string) map[string]string {
	items := strings.Split(env, ";")
	envMap := make(map[string]string)
	for _, item := range items {
		parts := strings.Split(item, "=")
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}
	return envMap
}

type authData struct {
	apiKey       string
	environment  map[string]string
	customerID   string
	customerName string
	collectorID  string
	valid        bool
	expiry       time.Time
}

var _ client.AuthData = (*authData)(nil)

func (a *authData) GetAttribute(name string) any {
	switch name {
	case "api_key":
		return a.apiKey
	case "environment":
		return a.environment
	case "customer_id":
		return a.customerID
	case "customer_name":
		return a.customerName
	case "collector_id":
		return a.collectorID
	case "valid":
		return a.valid
	default:
		return nil
	}
}

func (a *authData) GetAttributeNames() []string {
	return []string{"api_key", "environment", "customer_id", "customer_name", "collector_id", "valid"}
}
