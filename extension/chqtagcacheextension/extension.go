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

package chqtagcacheextension

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension/internal/metadata"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/wtcache"
)

type CHQTagcacheExtension struct {
	config             *Config
	httpClient         *http.Client
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	cacheGets          metric.Int64Counter
	cacheMisses        metric.Int64Counter
	cachePuts          metric.Int64Counter
	logger             *zap.Logger
	cache              wtcache.Cache
}

func (chq *CHQTagcacheExtension) setupTelemetry(params extension.Settings) error {
	m, err := metadata.Meter(params.TelemetrySettings).Int64Counter("cache_gets")
	if err != nil {
		return err
	}
	chq.cacheGets = m

	m, err = metadata.Meter(params.TelemetrySettings).Int64Counter("cache_misses")
	if err != nil {
		return err
	}
	chq.cacheMisses = m

	m, err = metadata.Meter(params.TelemetrySettings).Int64Counter("cache_puts")
	if err != nil {
		return err
	}
	chq.cachePuts = m

	return nil
}

func newConfigExtension(cfg *Config, params extension.Settings) (*CHQTagcacheExtension, error) {
	chq := CHQTagcacheExtension{
		config:             cfg,
		httpClientSettings: cfg.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		logger:             params.Logger,
	}
	if err := chq.setupTelemetry(params); err != nil {
		return nil, err
	}
	return &chq, nil
}

func (chq *CHQTagcacheExtension) Start(ctx context.Context, host component.Host) error {
	httpClient, err := chq.httpClientSettings.ToClient(ctx, host.GetExtensions(), chq.telemetrySettings)
	if err != nil {
		return err
	}
	chq.httpClient = httpClient

	chq.cache = wtcache.NewCache(
		wtcache.WithErrorTTL(chq.config.ErrorTTL),
		wtcache.WithTTL(chq.config.TTL),
		wtcache.WithFetcher(chq.tagFetcher),
		wtcache.WithPutter(chq.tagPutter),
	)

	return nil
}

func (chq *CHQTagcacheExtension) Shutdown(context.Context) error {
	return nil
}

// PutCache puts the tags in the cache.  This is intended to be called by the users of
// the extension.
func (chq *CHQTagcacheExtension) PutCache(key string, tags []Tag) error {
	chq.cachePuts.Add(context.Background(), 1)
	return chq.cache.Put(key, tags)
}

// FetchCache fetches the tags from the cache.  This is intended to be called by the users of
// the extension.
func (chq *CHQTagcacheExtension) FetchCache(key string) ([]Tag, error) {
	chq.cacheGets.Add(context.Background(), 1)
	tags, err := chq.cache.Get(key)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		return nil, nil
	}
	return tags.([]Tag), nil
}
