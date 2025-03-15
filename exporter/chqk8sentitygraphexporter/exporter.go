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

package chqk8sentitygraphexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler"
	"github.com/cardinalhq/oteltools/pkg/graph"
	"github.com/cardinalhq/oteltools/pkg/translate"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

type exp struct {
	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                 component.ID
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	cacheLock    sync.Mutex
	entityCaches map[string]*graph.ResourceEntityCache

	objecthandler objecthandler.ObjectHandler
	gee           objecthandler.GraphObjectEmitter
	goe           objecthandler.GraphEventEmitter
}

func newEntityGraphExporter(config *Config, set exporter.Settings) (*exp, error) {
	e := &exp{
		id:                 set.ID,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		entityCaches:       make(map[string]*graph.ResourceEntityCache),
		logger:             set.Logger,
	}

	return e, nil
}

func (e *exp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *exp) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

	e.gee = objecthandler.NewGraphObjectEmitter(e.logger, e.httpClient, e.config.Reporting.Interval)
	e.gee.Start(ctx)

	e.goe = objecthandler.NewGraphEventEmitter(e.logger, e.httpClient, e.config.Reporting.Interval)
	e.goe.Start(ctx)

	e.objecthandler = objecthandler.NewObjectHandler(e.logger, e.gee, e.goe)

	go func() {
		e.logger.Info("Starting entity graph exporter publish task")
		for {
			select {
			case <-ctx.Done():
				e.logger.Info("Stopping entity graph exporter publish task")
				return
			case <-time.Tick(e.config.Reporting.Interval):
				e.publishResourceEntities(ctx)
			}
		}
	}()

	return nil
}

func (e *exp) publishResourceEntities(ctx context.Context) {
	e.cacheLock.Lock()
	cids := make([]string, 0, len(e.entityCaches))
	for cid := range e.entityCaches {
		cids = append(cids, cid)
	}
	e.cacheLock.Unlock()

	for _, cid := range cids {
		e.publishResourceEntitiesForCID(ctx, cid)
	}
}

func (e *exp) publishResourceEntitiesForCID(ctx context.Context, cid string) {
	e.cacheLock.Lock()
	cache, found := e.entityCaches[cid]
	if !found {
		e.cacheLock.Unlock()
		return
	}
	protoEntities := cache.GetAllEntities()
	e.cacheLock.Unlock()
	if len(protoEntities) == 0 {
		return
	}

	if err := e.postEntityRelationships(ctx, cid, protoEntities); err != nil {
		e.logger.Error("Failed to send entity relationships", zap.Error(err))
	}
}

func urlFor(endpoint string, cid string) string {
	u, _ := url.Parse(endpoint)
	u.Path = "/api/v1/entityRelationships"
	q := u.Query()
	q.Add("organizationID", strings.ToLower(cid))
	u.RawQuery = q.Encode()
	return u.String()
}

func (e *exp) postEntityRelationships(ctx context.Context, cid string, payload []byte) error {
	endpoint := urlFor(e.config.Endpoint, cid)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/x-protobuf")

	if err != nil {
		return err
	}
	slog.Info("Sending entity relationships", slog.String("endpoint", endpoint), slog.Int("payloadSize", len(payload)))

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			e.logger.Error("Failed to close response body", zap.Error(closeErr))
		}
	}()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send resource entities",
			zap.String("endpoint", endpoint),
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)),
		)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (e *exp) postBackOffEvents(ctx context.Context, events []*graph.K8SPodObject) error {
	endpoint := e.config.Endpoint + "/api/v1/backOffEvents"
	b, err := json.Marshal(events)
	e.logger.Info("Sending backoff event", zap.String("endpoint", endpoint), zap.String("events", string(b)))
	if err != nil {
		return fmt.Errorf("failed to marshal kube events: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			e.logger.Warn("Request canceled", zap.Error(ctx.Err()))
			return nil
		}
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send kube events",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func orgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func (e *exp) getEntityCache(cid string) *graph.ResourceEntityCache {
	e.cacheLock.Lock()
	defer e.cacheLock.Unlock()
	cache, found := e.entityCaches[cid]
	if !found {
		cache = graph.NewResourceEntityCache()
		e.entityCaches[cid] = cache
	}
	return cache
}
