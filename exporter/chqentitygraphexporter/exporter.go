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

package chqentitygraphexporter

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cardinalhq/oteltools/pkg/graph"
	"github.com/cardinalhq/oteltools/pkg/translate"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

func newMarshaller() otelJsonMarshaller {
	return otelJsonMarshaller{
		logsMarshaler:    &plog.JSONMarshaler{},
		tracesMarshaler:  &ptrace.JSONMarshaler{},
		metricsMarshaler: &pmetric.JSONMarshaler{},
	}
}

type otelJsonMarshaller struct {
	logsMarshaler    plog.Marshaler
	tracesMarshaler  ptrace.Marshaler
	metricsMarshaler pmetric.Marshaler
}

type entityGraphExporter struct {
	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	cacheLock          sync.Mutex
	entityCaches       map[string]*graph.ResourceEntityCache
	spanExemplarCaches syncmap.SyncMap[string, *ShardedSpanLRUCache]

	jsonMarshaller otelJsonMarshaller
}

func newEntityGraphExporter(config *Config, ttype string, set exporter.Settings) (*entityGraphExporter, error) {
	e := &entityGraphExporter{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		jsonMarshaller:     newMarshaller(),
		entityCaches:       make(map[string]*graph.ResourceEntityCache),
		logger:             set.Logger,
	}

	return e, nil
}

func (e *entityGraphExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *entityGraphExporter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

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

func (e *entityGraphExporter) publishResourceEntities(ctx context.Context) {
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

func (e *entityGraphExporter) publishResourceEntitiesForCID(ctx context.Context, cid string) {
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

	if err := e.postEntityRelationships(ctx, e.ttype, cid, protoEntities); err != nil {
		e.logger.Error("Failed to send entity relationships", zap.Error(err))
	}
}

func urlFor(endpoint string, ttype string, cid string) string {
	u, _ := url.Parse(endpoint)
	u.Path = "/api/v1/entityRelationships"
	q := u.Query()
	q.Add("telemetryType", ttype)
	q.Add("organizationID", strings.ToLower(cid))
	u.RawQuery = q.Encode()
	return u.String()
}

func (e *entityGraphExporter) postEntityRelationships(ctx context.Context, ttype string, cid string, payload []byte) error {
	endpoint := urlFor(e.config.Endpoint, ttype, cid)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/x-protobuf")

	if err != nil {
		return err
	}
	slog.Info("Sending entity relationships",
		slog.String("endpoint", endpoint),
		slog.Int("payloadSize", len(payload)),
		slog.String("signal", ttype),
		slog.String("cid", cid))

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

func orgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func (e *entityGraphExporter) getEntityCache(cid string) *graph.ResourceEntityCache {
	e.cacheLock.Lock()
	defer e.cacheLock.Unlock()
	cache, found := e.entityCaches[cid]
	if !found {
		cache = graph.NewResourceEntityCache()
		e.entityCaches[cid] = cache
	}
	return cache
}
