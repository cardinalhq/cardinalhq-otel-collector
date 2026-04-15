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
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/otel/metric/noop"
)

func newchq() *chqServerAuth {
	m := noop.NewMeterProvider().Meter("test")
	clm, _ := m.Int64Counter("auth_cache_lookups")
	cam, _ := m.Int64Counter("auth_cache_adds")

	chq := &chqServerAuth{
		lookupCache:      make(map[string]*authData),
		authCacheLookups: clm,
		authCacheAdds:    cam,
	}
	return chq
}

func TestGetCache(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name            string
		contents        []*authData
		query           string
		expected        *authData
		expectedExpired bool
	}{
		{
			"cache starts empty",
			[]*authData{},
			"key1",
			nil,
			false,
		},
		{
			"cache hit",
			[]*authData{
				{
					apiKey: "key1",
					valid:  true,
					expiry: now.Add(time.Hour),
				},
			},
			"key1",
			&authData{
				apiKey: "key1",
				valid:  true,
				expiry: now.Add(time.Hour),
			},
			false,
		},
		{
			"item is expired",
			[]*authData{
				{
					apiKey: "key1",
					valid:  true,
					expiry: now.Add(-time.Hour),
				},
			},
			"key1",
			&authData{
				apiKey: "key1",
				valid:  true,
				expiry: now.Add(-time.Hour),
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chq := newchq()
			for _, ad := range tt.contents {
				chq.lookupCache[ad.apiKey] = ad
			}
			ad, expired := chq.getcache(tt.query)
			assert.Equal(t, tt.expected, ad)
			assert.Equal(t, tt.expectedExpired, expired)
		})
	}
}

func TestSetCache(t *testing.T) {
	chq := newchq()
	ad := &authData{
		apiKey:       "key1",
		customerID:   "client1",
		customerName: "John Doe",
		valid:        true,
		expiry:       time.Now().Add(time.Hour),
	}

	chq.setcache(ad)
	assert.Equal(t, ad, chq.lookupCache[ad.apiKey])
}

func TestGetAttribute(t *testing.T) {
	ad := &authData{
		apiKey:       "key1",
		customerID:   "client1",
		customerName: "John Doe",
		valid:        true,
		expiry:       time.Now().Add(time.Hour),
	}

	tests := []struct {
		field    string
		expected any
	}{
		{
			"api_key",
			"key1",
		},
		{
			"customer_id",
			"client1",
		},
		{
			"customer_name",
			"John Doe",
		},
		{
			"valid",
			true,
		},
		{
			"unknown",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			assert.Equal(t, tt.expected, ad.GetAttribute(tt.field))
		})
	}
}

func TestGetAttributeNames(t *testing.T) {
	ad := &authData{}
	expected := []string{"api_key", "customer_id", "customer_name", "valid"}
	names := ad.GetAttributeNames()
	assert.ElementsMatch(t, expected, names)
}

func TestGetAuthHeader(t *testing.T) {
	tests := []struct {
		name     string
		targets  []string
		headers  map[string][]string
		expected string
	}{
		{
			"no header",
			[]string{apiKeyHeader},
			map[string][]string{},
			"",
		},
		{
			"header not found",
			[]string{apiKeyHeader},
			map[string][]string{
				"Content-Type": {"application/json"},
			},
			"",
		},
		{
			"header found",
			[]string{apiKeyHeader},
			map[string][]string{
				"x-cardinalhq-api-key": {"my-api-key"},
				"Content-Type":         {"application/json"},
			},
			"my-api-key",
		},
		{
			"case insensitive",
			[]string{apiKeyHeader},
			map[string][]string{
				"X-CARDINALHQ-API-KEY": {"my-api-key"},
				"Content-Type":         {"application/json"},
			},
			"my-api-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authHeader := getAuthHeader(tt.targets, tt.headers)
			assert.Equal(t, tt.expected, authHeader)
		})
	}
}
// newchqWithServer returns a chqServerAuth wired against a mock validator
// whose JSON body is controlled by the test. The returned cleanup closes
// the server.
func newchqWithServer(t *testing.T, handler http.HandlerFunc) (*chqServerAuth, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)

	chq := newchq()
	chq.config = &Config{ServerAuth: &ServerAuth{
		ClientConfig:    confighttp.ClientConfig{Endpoint: srv.URL},
		CacheTTLValid:   10 * time.Minute,
		CacheTTLInvalid: 1 * time.Minute,
		Headers:         []string{"x-cardinalhq-api-key"},
	}}
	chq.httpClient = srv.Client()
	return chq, srv
}

func TestCallValidateAPI_RejectsInvalidOrEmptyCustomer(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantDenied bool
	}{
		{
			name:       "valid=true with customer_id is accepted",
			status:     http.StatusOK,
			body:       `{"valid":true,"customer_id":"cust-1","customer_name":"n","collector_id":"col","collector_name":"cn"}`,
			wantDenied: false,
		},
		{
			name:       "valid=true but empty customer_id is denied",
			status:     http.StatusOK,
			body:       `{"valid":true,"customer_id":""}`,
			wantDenied: true,
		},
		{
			name:       "valid=false with 200 OK is denied",
			status:     http.StatusOK,
			body:       `{"valid":false,"customer_id":"cust-1"}`,
			wantDenied: true,
		},
		{
			name:       "valid=false and empty customer_id is denied",
			status:     http.StatusOK,
			body:       `{"valid":false,"customer_id":""}`,
			wantDenied: true,
		},
		{
			name:       "non-200 is denied",
			status:     http.StatusForbidden,
			body:       ``,
			wantDenied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chq, srv := newchqWithServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})
			defer srv.Close()

			ad, err := chq.callValidateAPI(context.Background(), "key")
			if tt.wantDenied {
				assert.Nil(t, ad)
				assert.ErrorIs(t, err, errDenied)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ad)
			assert.Equal(t, "cust-1", ad.customerID)
			assert.True(t, ad.valid)
		})
	}
}

func TestAuthenticateAPIKey_RevokedKeyDoesNotReturnStaleCache(t *testing.T) {
	// This test guards against a regression where a revoked key, after
	// the cached entry expired, would be re-authenticated as the old
	// customer for one more TTL-boundary crossing.
	var denyNext atomic.Bool
	chq, srv := newchqWithServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if denyNext.Load() {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"valid":true,"customer_id":"cust-1","collector_id":"col"}`))
	})
	defer srv.Close()

	// First call: populate cache with a valid entry.
	ad, err := chq.authenticateAPIKey(context.Background(), "key")
	require.NoError(t, err)
	require.NotNil(t, ad)
	assert.Equal(t, "cust-1", ad.customerID)

	// Force the cached entry to look expired.
	chq.cacheLock.Lock()
	chq.lookupCache["key"].expiry = time.Now().Add(-time.Second)
	chq.cacheLock.Unlock()

	// Validator now denies the key (revoked).
	denyNext.Store(true)

	// Second call must return errDenied, not the previously valid authData.
	ad2, err := chq.authenticateAPIKey(context.Background(), "key")
	assert.Nil(t, ad2)
	assert.True(t, errors.Is(err, errDenied), "expected errDenied, got %v", err)

	// And the cache must now hold a valid=false entry so future calls
	// within the invalid TTL are rejected without hitting the validator.
	chq.cacheLock.Lock()
	neg, ok := chq.lookupCache["key"]
	chq.cacheLock.Unlock()
	require.True(t, ok, "expected a negative cache entry after denial")
	assert.False(t, neg.valid)
}

func TestAuthenticateAPIKey_TransientErrorFallsBackToCache(t *testing.T) {
	// Verifies that genuine transient errors (network failures,
	// parse errors) still allow the last-known cached entry to
	// serve the request — the availability trade-off is retained.
	// Note: the current implementation treats any non-200 as
	// errDenied, so only connection-level failures exercise this
	// branch.
	chq, srv := newchqWithServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"valid":true,"customer_id":"cust-1","collector_id":"col"}`))
	})

	ad, err := chq.authenticateAPIKey(context.Background(), "key")
	require.NoError(t, err)
	require.NotNil(t, ad)

	// Expire the cached entry.
	chq.cacheLock.Lock()
	chq.lookupCache["key"].expiry = time.Now().Add(-time.Second)
	chq.cacheLock.Unlock()

	// Take the validator offline — httpClient.Do will surface a
	// non-errDenied error, which must fall back to the cached entry.
	srv.Close()

	ad2, err := chq.authenticateAPIKey(context.Background(), "key")
	require.NoError(t, err)
	require.NotNil(t, ad2)
	assert.Equal(t, "cust-1", ad2.customerID)
}

