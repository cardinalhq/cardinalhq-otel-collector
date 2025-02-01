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

package chqstatsprocessor

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/graph"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqstatsprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/ottl"
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

type statsProc struct {
	config          *Config
	httpClient      *http.Client
	logger          *zap.Logger
	configExtension *chqconfigextension.CHQConfigExtension

	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	pbPhase            chqpb.Phase
	podName            string

	idsFromEnv bool

	configCallbackID int

	logstats    *chqpb.EventStatsCache
	spanStats   *chqpb.EventStatsCache
	metricstats *chqpb.MetricStatsCache

	logExemplars    *LRUCache
	traceExemplars  *LRUCache
	metricExemplars *LRUCache

	logsEntityCache    *graph.ResourceEntityCache
	metricsEntityCache *graph.ResourceEntityCache
	tracesEntityCache  *graph.ResourceEntityCache

	jsonMarshaller otelJsonMarshaller

	logStatsEnrichments     atomic.Pointer[[]ottl.StatsEnrichment]
	metricsStatsEnrichments atomic.Pointer[[]ottl.StatsEnrichment]
	tracesStatsEnrichments  atomic.Pointer[[]ottl.StatsEnrichment]
	statsBatchSize          telemetry.DeferrableHistogram
	recordLatency           telemetry.DeferrableHistogram
	cacheFull               telemetry.DeferrableCounter

	enableMetricMetrics bool
	enableLogMetrics    bool
}

func newStatsProc(config *Config, ttype string, set processor.Settings) (*statsProc, error) {
	dog := &statsProc{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		jsonMarshaller:     newMarshaller(),
		logExemplars:       NewLRUCache(1000, 5*time.Minute),
		traceExemplars:     NewLRUCache(1000, 5*time.Minute),
		metricExemplars:    NewLRUCache(1000, 5*time.Minute),
		logsEntityCache:    graph.NewResourceEntityCache(),
		metricsEntityCache: graph.NewResourceEntityCache(),
		tracesEntityCache:  graph.NewResourceEntityCache(),
		logger:             set.Logger,
		podName:            os.Getenv("POD_NAME"),
	}

	//if os.Getenv("ENABLE_METRIC_METRICS") == "true" {
	dog.enableMetricMetrics = true
	//}
	//if os.Getenv("ENABLE_LOG_METRICS") == "true" {
	dog.enableLogMetrics = true
	//}

	if config.Statistics.Phase == "presample" {
		dog.pbPhase = chqpb.Phase_PRE
	} else {
		dog.pbPhase = chqpb.Phase_POST
	}

	processorId := set.ID.String()
	clock := &chqpb.RealClock{}
	capacity := 1000
	switch ttype {
	case "logs":
		dog.logstats = chqpb.NewEventStatsCache(capacity, 16, 5*time.Minute, dog.sendLogStats, chqpb.InitializeEventStats, clock)
		dog.logstats.Start()
		dog.logger.Info("Initialized LogStats Combiner", zap.Duration("interval", config.Statistics.Interval))
	case "metrics":
		dog.metricstats = chqpb.NewMetricStatsCache(capacity, 16, 5*time.Minute, dog.sendMetricStats, chqpb.InitializeMetricStats, clock)
		dog.metricstats.Start()
		dog.logger.Info("Initialized MetricStatsCache", zap.Duration("interval", config.Statistics.Interval))
	case "traces":
		dog.spanStats = chqpb.NewEventStatsCache(capacity, 16, 5*time.Minute, dog.sendSpanStats, chqpb.InitializeEventStats, clock)
		dog.spanStats.Start()
		dog.logger.Info("Initialized SpanStats Combiner", zap.Duration("interval", config.Statistics.Interval))
	}

	attrset := attribute.NewSet(
		attribute.String("processor", processorId),
		attribute.String("signal", ttype),
	)
	histogram, histogramError := telemetry.NewDeferrableHistogram(metadata.Meter(set.TelemetrySettings),
		"stats_batch_size",
		[]metric.Int64HistogramOption{},
		[]metric.RecordOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if histogramError != nil {
		return nil, histogramError
	}
	dog.statsBatchSize = histogram

	cacheFullCounter, cacheFullErr := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"cache_full",
		[]metric.Int64CounterOption{},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if cacheFullErr != nil {
		return nil, histogramError
	}
	dog.cacheFull = cacheFullCounter

	recordLatencyHistogram, recordLatencyHistogramError := telemetry.NewDeferrableHistogram(metadata.Meter(set.TelemetrySettings),
		"record_latency",
		[]metric.Int64HistogramOption{},
		[]metric.RecordOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if recordLatencyHistogramError != nil {
		return nil, recordLatencyHistogramError
	}
	dog.recordLatency = recordLatencyHistogram

	return dog, nil
}

func (e *statsProc) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *statsProc) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

	ext, found := host.GetExtensions()[*e.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	e.configExtension = cext
	e.configCallbackID = e.configExtension.RegisterCallback(e.id.String()+"/"+e.ttype, e.configUpdateCallback)

	e.idsFromEnv = e.config.IDSource == "env"

	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.publishResourceEntities(ctx)
			}
		}
	}()

	return nil
}

func (e *statsProc) Shutdown(ctx context.Context) error {
	var errors *multierror.Error
	e.configExtension.UnregisterCallback(e.configCallbackID)
	return errors.ErrorOrNil()
}

func (e *statsProc) processEnrichments(attributesByScope map[string]pcommon.Map) []*chqpb.Attribute {
	tags := make([]*chqpb.Attribute, 0)
	var enrichments *[]ottl.StatsEnrichment
	switch e.ttype {
	case "logs":
		enrichments = e.logStatsEnrichments.Load()
	case "metrics":
		enrichments = e.metricsStatsEnrichments.Load()
	case "traces":
		enrichments = e.tracesStatsEnrichments.Load()
	}

	if enrichments != nil {
		for _, enrichment := range *enrichments {
			attributes, found := attributesByScope[enrichment.Context]
			if found {
				for _, tag := range enrichment.Tags {
					if tagValue, found := attributes.Get(tag); found {
						tags = append(tags, toAttribute(enrichment.Context, tag, tagValue, false))
					}
				}
			}
		}
	}
	return tags
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

func (e *statsProc) configUpdateCallback(cpc ottl.ControlPlaneConfig) {
	configs := cpc.Stats[e.id.Name()]
	if configs == nil {
		configs = cpc.Stats[uuid.Nil.String()]
	}
	if configs == nil {
		return
	}

	switch e.ttype {
	case "logs":
		e.logStatsEnrichments.Store(&configs.LogEnrichments)
		e.logger.Info("Stats enrichment for logs", zap.String("instance", e.id.Name()), zap.Int("enrichments", len(configs.LogEnrichments)))
	case "metrics":
		e.metricsStatsEnrichments.Store(&configs.MetricEnrichments)
		e.logger.Info("Stats enrichment for metrics", zap.String("instance", e.id.Name()), zap.Int("enrichments", len(configs.MetricEnrichments)))
	case "traces":
		e.tracesStatsEnrichments.Store(&configs.SpanEnrichments)
		e.logger.Info("Stats enrichment for traces", zap.String("instance", e.id.Name()), zap.Int("enrichments", len(configs.SpanEnrichments)))
	}

	e.logger.Info("Configuration updated for processor instance", zap.String("instance", e.id.Name()))
}

func hashString(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64())
}

func (e *statsProc) toExemplarKey(serviceName string, fingerprint int64) int64 {
	return hashString(fmt.Sprintf("%s-%d", serviceName, fingerprint))
}

func (e *statsProc) publishResourceEntities(ctx context.Context) {
	var cache *graph.ResourceEntityCache
	switch e.ttype {
	case "logs":
		cache = e.logsEntityCache
	case "metrics":
		cache = e.metricsEntityCache
	case "traces":
		cache = e.tracesEntityCache
	default:
		e.logger.Error("Unknown processor type", zap.String("type", e.ttype))
		return
	}

	protoEntities := cache.GetAllEntities()
	if len(protoEntities) == 0 {
		return
	}

	for _, protoEntity := range protoEntities {
		if err := e.postEntityRelationships(ctx, e.ttype, protoEntity); err != nil {
			e.logger.Error("Failed to send entity relationships", zap.Error(err))
		}
	}
}

func (e *statsProc) postEntityRelationships(ctx context.Context, ttype string, protoEntity []byte) error {

	var compressedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedData)
	if _, err := gzipWriter.Write(protoEntity); err != nil {
		return err
	}
	if err := gzipWriter.Close(); err != nil {
		return err
	}

	endpoint := e.config.Endpoint + fmt.Sprintf("%s?telemetryType=%s", "/api/v1/entityRelationships", ttype)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &compressedData)
	if err != nil {
		return err
	}
	slog.Info("Sending entity relationships", slog.String("endpoint", endpoint), slog.Int("payloadSize", len(protoEntity)))

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

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
		slog.Error("Failed to send resource entities",
			slog.Int("status", resp.StatusCode),
			zap.String("body", string(body)),
		)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
