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

package chqrelationshipsprocessor

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/graph"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
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

type statsProcessor struct {
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
}

func newRelationshiosProcessor(config *Config, ttype string, set processor.Settings) (*statsProcessor, error) {
	p := &statsProcessor{
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

	return p, nil
}

func (p *statsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *statsProcessor) Start(ctx context.Context, host component.Host) error {
	httpClient, err := p.httpClientSettings.ToClient(ctx, host, p.telemetrySettings)
	if err != nil {
		return err
	}
	p.httpClient = httpClient

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.Tick(p.config.Reporting.Interval):
				p.publishResourceEntities(ctx)
			}
		}
	}()

	return nil
}

func (p *statsProcessor) Shutdown(ctx context.Context) error {
	return nil // TODO shut down the goproc started in Start()
}

func toAttribute(contextId string, k string, v pcommon.Value, isAttribute bool) *chqpb.Attribute {
	return &chqpb.Attribute{
		ContextId:   contextId,
		IsAttribute: isAttribute,
		Type:        int32(v.Type()),
		Key:         k,
		Value:       v.AsString(),
	}
}

func hashString(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64())
}

func (p *statsProcessor) toExemplarKey(serviceName string, fingerprint int64) int64 {
	return hashString(fmt.Sprintf("%s-%d", serviceName, fingerprint))
}

func (p *statsProcessor) publishResourceEntities(ctx context.Context) {
	var cache *graph.ResourceEntityCache
	switch p.ttype {
	case "logs":
		cache = p.logsEntityCache
	case "metrics":
		cache = p.metricsEntityCache
	case "traces":
		cache = p.tracesEntityCache
	default:
		p.logger.Error("Unknown processor type", zap.String("type", p.ttype))
		return
	}

	protoEntities := cache.GetAllEntities()
	if len(protoEntities) == 0 {
		return
	}
	if err := p.postEntityRelationships(ctx, p.ttype, protoEntities); err != nil {
		p.logger.Error("Failed to send entity relationships", zap.Error(err))
	}
}

func (p *statsProcessor) postEntityRelationships(ctx context.Context, ttype string, payload []byte) error {
	var compressedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedData)
	if _, err := gzipWriter.Write(payload); err != nil {
		return err
	}
	if err := gzipWriter.Close(); err != nil {
		return err
	}

	endpoint := p.config.Endpoint + fmt.Sprintf("%s?telemetryType=%s", "/api/v1/entityRelationships", ttype)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &compressedData)
	if err != nil {
		return err
	}
	slog.Info("Sending entity relationships", slog.String("endpoint", endpoint), slog.Int("payloadSize", len(payload)))

	req.Header.Set("Content-Encoding", "gzip")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			p.logger.Error("Failed to close response body", zap.Error(closeErr))
		}
	}()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send resource entities",
			zap.String("endpoint", endpoint),
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)),
		)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
