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
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/graph"

	"go.opentelemetry.io/collector/exporter"
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

	logsEntityCache    *graph.ResourceEntityCache
	metricsEntityCache *graph.ResourceEntityCache
	tracesEntityCache  *graph.ResourceEntityCache

	jsonMarshaller otelJsonMarshaller
	idSource       authenv.EnvironmentSource
}

func newEntityGraphExporter(config *Config, ttype string, set exporter.Settings) (*entityGraphExporter, error) {
	e := &entityGraphExporter{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		jsonMarshaller:     newMarshaller(),
		logsEntityCache:    graph.NewResourceEntityCache(),
		metricsEntityCache: graph.NewResourceEntityCache(),
		tracesEntityCache:  graph.NewResourceEntityCache(),
		logger:             set.Logger,
	}

	idsource, err := authenv.ParseEnvironmentSource(config.IDSource)
	if err != nil {
		return nil, err
	}
	e.idSource = idsource

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
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.Tick(e.config.Reporting.Interval):
				e.publishResourceEntities(ctx)
			}
		}
	}()

	return nil
}

func (e *entityGraphExporter) Shutdown(ctx context.Context) error {
	return nil // TODO shut down the goproc started in Start()
}

func (e *entityGraphExporter) publishResourceEntities(ctx context.Context) {
	var cache *graph.ResourceEntityCache
	switch e.ttype {
	case "logs":
		cache = e.logsEntityCache
	case "metrics":
		cache = e.metricsEntityCache
	case "traces":
		cache = e.tracesEntityCache
	default:
		e.logger.Error("Unknown type", zap.String("type", e.ttype))
		return
	}

	protoEntities := cache.GetAllEntities()
	if len(protoEntities) == 0 {
		return
	}
	if err := e.postEntityRelationships(ctx, e.ttype, protoEntities); err != nil {
		e.logger.Error("Failed to send entity relationships", zap.Error(err))
	}
}

func (e *entityGraphExporter) postEntityRelationships(ctx context.Context, ttype string, payload []byte) error {
	endpoint := e.config.Endpoint + fmt.Sprintf("%s?telemetryType=%s", "/api/v1/entityRelationships", ttype)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
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
