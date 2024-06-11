package chqauthextension

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/noop"
)

func newchq() *chqAuth {
	m := noop.NewMeterProvider().Meter("test")
	clm, _ := m.Int64Counter("auth_cache_lookups")
	cam, _ := m.Int64Counter("auth_cache_adds")

	chq := &chqAuth{
		lookupCache:      make(map[string]*authData),
		authCacheLookups: clm,
		authCacheAdds:    cam,
	}
	return chq
}

func TestGetCache(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		contents []*authData
		query    string
		expected *authData
	}{
		{
			"cache starts empty",
			[]*authData{},
			"key1",
			nil,
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
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chq := newchq()
			for _, ad := range tt.contents {
				chq.lookupCache[ad.apiKey] = ad
			}
			ad := chq.getcache(tt.query)
			assert.Equal(t, tt.expected, ad)
		})
	}
}

func TestSetCache(t *testing.T) {
	chq := newchq()
	ad := &authData{
		apiKey:     "key1",
		clientID:   "client1",
		clientName: "John Doe",
		valid:      true,
		expiry:     time.Now().Add(time.Hour),
	}

	chq.setcache(ad)
	assert.Equal(t, ad, chq.lookupCache["key1"])
}

func TestGetAttribute(t *testing.T) {
	ad := &authData{
		apiKey:     "key1",
		clientID:   "client1",
		clientName: "John Doe",
		valid:      true,
		expiry:     time.Now().Add(time.Hour),
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
			"client_id",
			"client1",
		},
		{
			"client_name",
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
	expected := []string{"api_key", "client_id", "client_name", "valid"}
	names := ad.GetAttributeNames()
	assert.ElementsMatch(t, expected, names)
}

func TestGetAuthHeader(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string][]string
		expected string
	}{
		{
			"no header",
			map[string][]string{},
			"",
		},
		{
			"header not found",
			map[string][]string{
				"Content-Type": {"application/json"},
			},
			"",
		},
		{
			"header found",
			map[string][]string{
				"x-cardinalhq-api-key": {"my-api-key"},
				"Content-Type":         {"application/json"},
			},
			"my-api-key",
		},
		{
			"case insensitive",
			map[string][]string{
				"X-CARDINALHQ-API-KEY": {"my-api-key"},
				"Content-Type":         {"application/json"},
			},
			"my-api-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authHeader := getAuthHeader(tt.headers)
			assert.Equal(t, tt.expected, authHeader)
		})
	}
}
