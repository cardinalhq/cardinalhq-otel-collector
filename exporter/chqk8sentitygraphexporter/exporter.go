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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
	"github.com/cardinalhq/oteltools/pkg/graph"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

type exp struct {
	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                 component.ID
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	objecthandler  objecthandler.ObjectHandler
	k8sClusterName string

	cacheLock    sync.Mutex
	entityCaches map[string]*graph.ResourceEntityCache
}

func newEntityGraphExporter(config *Config, set exporter.Settings) (*exp, error) {
	e := &exp{
		id:                 set.ID,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		k8sClusterName:     os.Getenv("K8S_CLUSTER_NAME"),
		entityCaches:       make(map[string]*graph.ResourceEntityCache),
	}

	return e, nil
}

func (e *exp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *exp) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host.GetExtensions(), e.telemetrySettings)
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

	cconf := converterconfig.New(e.k8sClusterName).WithHashItems(e.k8sClusterName)
	e.objecthandler = objecthandler.NewObjectHandler(cconf)

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

	if err := e.postEntityRelationships(ctx, "logs", cid, protoEntities); err != nil {
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

func (e *exp) postEntityRelationships(ctx context.Context, ttype string, cid string, payload []byte) error {
	endpoint := urlFor(e.config.Endpoint, ttype, cid)

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
