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

package chqservicegraphexporter

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type serviceGraphExporter struct {
	config     *Config
	httpClient *http.Client

	edgeCaches    map[string]*EdgeCache
	edgeCacheLock sync.Mutex

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	exportedEdges metric.Int64Counter

	logger *zap.Logger
}

func newServiceGraphExporter(config *Config, params exporter.Settings) *serviceGraphExporter {
	e := &serviceGraphExporter{
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  params.TelemetrySettings,
		edgeCaches:         map[string]*EdgeCache{},
		logger:             params.Logger,
	}

	p := params.MeterProvider.Meter("otelcol/chqservicegraph")

	received, err := p.Int64Counter("exported_edges",
		metric.WithDescription("The number of edges exported"))
	if err != nil {
		e.logger.Error("Failed to create metric", zap.Error(err))
	}
	e.exportedEdges = received

	return e
}

func (e *serviceGraphExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *serviceGraphExporter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient
	return nil
}

func OrgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func (e *serviceGraphExporter) getEdgeCache(orgID string) *EdgeCache {
	e.edgeCacheLock.Lock()
	defer e.edgeCacheLock.Unlock()

	edgeCache, found := e.edgeCaches[orgID]
	if !found {
		edgeCache = NewEdgeCache(30*time.Second, func() int64 {
			return time.Now().UnixNano()
		})
		e.edgeCaches[orgID] = edgeCache
	}
	return edgeCache
}
