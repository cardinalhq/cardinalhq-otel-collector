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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension/internal/metadata"
)

type chqServerAuth struct {
	component.StartFunc
	component.ShutdownFunc

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

	debugLogInvalidKeys   bool
	invalidKeyLogLock     sync.Mutex
	invalidKeyLogLastSeen map[string]time.Time
}

const invalidKeyLogInterval = 5 * time.Minute

var (
	_ extension.Extension  = (*chqServerAuth)(nil)
	_ extensionauth.Server = (*chqServerAuth)(nil)
)

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

func newServerAuthExtension(cfg *Config, params extension.Settings) (*chqServerAuth, error) {
	chq := chqServerAuth{
		config:                cfg,
		httpClientSettings:    cfg.ServerAuth.ClientConfig,
		telemetrySettings:     params.TelemetrySettings,
		logger:                params.Logger,
		lookupCache:           make(map[string]*authData),
		debugLogInvalidKeys:   strings.EqualFold(os.Getenv("CHQAUTH_SERVER_DEBUG"), "true"),
		invalidKeyLogLastSeen: make(map[string]time.Time),
	}
	if err := chq.setupServerTelemetry(params); err != nil {
		return nil, err
	}
	return &chq, nil
}

func (chq *chqServerAuth) Start(_ context.Context, _ component.Host) error {
	httpClient, err := chq.httpClientSettings.ToClient(context.Background(), nil, chq.telemetrySettings)
	if err != nil {
		return err
	}
	chq.httpClient = httpClient
	return nil
}

func (chq *chqServerAuth) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	auth := getAuthHeader(chq.config.ServerAuth.Headers, headers)
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
	CustomerID   string `json:"customer_id"`
	CustomerName string `json:"customer_name"`
	Valid        bool   `json:"valid"`
}

func (chq *chqServerAuth) getcache(apiKey string) (*authData, bool) {
	chq.cacheLock.Lock()
	defer chq.cacheLock.Unlock()
	ad, ok := chq.lookupCache[apiKey]
	if !ok {
		attrs := metric.WithAttributes(attribute.String("cache", "miss"))
		chq.authCacheLookups.Add(context.Background(), 1, attrs)
		return nil, false
	}
	if ad.expiry.Before(time.Now()) {
		attrs := metric.WithAttributes(attribute.String("cache", "expired"))
		chq.authCacheLookups.Add(context.Background(), 1, attrs)
		delete(chq.lookupCache, apiKey)
		return ad, true
	}
	attrs := metric.WithAttributes(attribute.String("cache", "hit"))
	chq.authCacheLookups.Add(context.Background(), 1, attrs)
	return ad, false
}

func (chq *chqServerAuth) setcache(ad *authData) {
	chq.authCacheAdds.Add(context.Background(), 1)
	chq.cacheLock.Lock()
	defer chq.cacheLock.Unlock()
	chq.lookupCache[ad.apiKey] = ad
}

func (chq *chqServerAuth) maybeLogInvalidKey(apiKey string) {
	if !chq.debugLogInvalidKeys {
		return
	}
	chq.invalidKeyLogLock.Lock()
	now := time.Now()
	last, ok := chq.invalidKeyLogLastSeen[apiKey]
	if ok && now.Sub(last) < invalidKeyLogInterval {
		chq.invalidKeyLogLock.Unlock()
		return
	}
	chq.invalidKeyLogLastSeen[apiKey] = now
	chq.invalidKeyLogLock.Unlock()
	chq.logger.Info("invalid API key attempted", zap.String("api_key", apiKey))
}

func (chq *chqServerAuth) authenticateAPIKey(ctx context.Context, apiKey string) (*authData, error) {
	cached, expired := chq.getcache(apiKey)
	if cached != nil && !expired {
		if !cached.valid {
			chq.maybeLogInvalidKey(apiKey)
			return nil, errDenied
		}
		return cached, nil
	}

	ad, err := chq.callValidateAPI(ctx, apiKey)
	if err != nil {
		if errors.Is(err, errDenied) {
			chq.setcache(&authData{
				apiKey: apiKey,
				valid:  false,
				expiry: time.Now().Add(chq.config.ServerAuth.CacheTTLInvalid),
			})
			chq.maybeLogInvalidKey(apiKey)
			// A definitive denial must never fall back to a previously
			// cached valid entry — that would let a revoked key keep
			// authenticating as its former customer for one more TTL
			// boundary crossing.
			return nil, errDenied
		}

		// Transient error (network, 5xx, parse): fall back to the last
		// known cached entry if we have one, to preserve availability.
		if cached != nil {
			chq.logger.Warn("auth validator transient failure; serving expired cache entry",
				zap.Error(err))
			return cached, nil
		}
		chq.logger.Warn("auth validator transient failure; no cache available",
			zap.Error(err))
		return nil, err
	}
	ad.expiry = time.Now().Add(chq.config.ServerAuth.CacheTTLValid)
	chq.setcache(ad)
	return ad, nil
}

func (chq *chqServerAuth) callValidateAPI(ctx context.Context, apiKey string) (*authData, error) {
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
	defer func() {
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, errDenied
	}

	var validateResp validateResponse
	if err := json.NewDecoder(resp.Body).Decode(&validateResp); err != nil {
		return nil, err
	}

	// A response must positively assert validity and carry a non-empty
	// customer_id before we treat it as an authenticated identity.
	// Without this, a 200 OK carrying {"valid":true,"customer_id":""}
	// (or {"valid":false,...}) would be accepted, cached under the
	// valid-TTL, and every downstream request would flow with an empty
	// tenant — resulting in data written to paths that lack a customer
	// UUID segment.
	if !validateResp.Valid || validateResp.CustomerID == "" {
		if validateResp.Valid && validateResp.CustomerID == "" {
			// Upstream contract violation: treated as a denial below, but
			// log it distinctly so it doesn't masquerade as a normal revocation.
			chq.logger.Error("auth validator returned valid=true with empty customer_id")
		}
		return nil, errDenied
	}

	return &authData{
		apiKey:       apiKey,
		customerID:   validateResp.CustomerID,
		customerName: validateResp.CustomerName,
		valid:        validateResp.Valid,
	}, nil
}

func getAuthHeader(targets []string, h map[string][]string) string {
	for _, key := range targets {
		for k, v := range h {
			if strings.EqualFold(k, key) {
				return v[0]
			}
		}
	}
	return ""
}

type authData struct {
	apiKey       string
	customerID   string
	customerName string
	valid        bool
	expiry       time.Time
}

var _ client.AuthData = (*authData)(nil)

func (a *authData) GetAttribute(name string) any {
	switch name {
	case "api_key":
		return a.apiKey
	case "customer_id":
		return a.customerID
	case "customer_name":
		return a.customerName
	case "valid":
		return a.valid
	default:
		return nil
	}
}

func (a *authData) GetAttributeNames() []string {
	return []string{"api_key", "customer_id", "customer_name", "valid"}
}
