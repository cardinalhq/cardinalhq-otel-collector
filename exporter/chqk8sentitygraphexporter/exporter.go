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
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"

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

	gee, err := objecthandler.NewGraphObjectEmitter(e.logger, e.httpClient, e.config.Reporting.Interval, e.config.Endpoint)
	if err != nil {
		return err
	}
	e.gee = gee

	e.goe = objecthandler.NewGraphEventEmitter(e.logger, e.httpClient, e.config.Reporting.Interval, e.config.Endpoint)

	e.objecthandler = objecthandler.NewObjectHandler()

	e.gee.Start(ctx)
	e.goe.Start(ctx)

	return nil
}

func urlFor(endpoint string, cid string) string {
	u, _ := url.Parse(endpoint)
	u.Path = "/api/v1/entityRelationships"
	q := u.Query()
	q.Add("organizationID", strings.ToLower(cid))
	u.RawQuery = q.Encode()
	return u.String()
}

func orgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}
