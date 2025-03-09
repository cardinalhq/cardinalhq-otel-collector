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
	"context"
	"errors"
	"github.com/cardinalhq/oteltools/pkg/stats"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqstatsprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
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

type statsProcessor struct {
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

	idSource authenv.EnvironmentSource

	configCallbackID int

	tenants    map[string]*Tenant
	tenantLock sync.Mutex

	jsonMarshaller otelJsonMarshaller

	loadedConfig   atomic.Pointer[ottl.ControlPlaneConfig]
	statsBatchSize telemetry.DeferrableHistogram[int64]
	recordLatency  telemetry.DeferrableHistogram[int64]
	cacheFull      telemetry.DeferrableCounter[int64]
}

type Tenant struct {
	logExemplars    *LRUCache
	traceExemplars  *LRUCache
	metricExemplars *LRUCache

	spanSketches *stats.SketchCache
}

func (p *statsProcessor) getTenant(organizationID string) *Tenant {
	p.tenantLock.Lock()
	defer p.tenantLock.Unlock()
	tenant, found := p.tenants[organizationID]
	if !found {
		tenant = &Tenant{}
		switch p.ttype {
		case "logs":
			tenant.logExemplars = NewLRUCache(1000, 30*time.Minute, p.sendExemplars(organizationID, p.ttype, p.id.Name()))
		case "metrics":
			tenant.metricExemplars = NewLRUCache(1000, 30*time.Minute, p.sendExemplars(organizationID, p.ttype, p.id.Name()))
		case "traces":
			tenant.traceExemplars = NewLRUCache(1000, 30*time.Minute, p.sendExemplars(organizationID, p.ttype, p.id.Name()))
			tenant.spanSketches = stats.NewSketchCache(1*time.Minute, p.sendSpanSketchesFor(organizationID))
		}

		p.tenants[organizationID] = tenant
	}

	return tenant
}

func newStatsProcessor(config *Config, ttype string, set processor.Settings) (*statsProcessor, error) {
	p := &statsProcessor{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		jsonMarshaller:     newMarshaller(),
		logger:             set.Logger,
		podName:            os.Getenv("POD_NAME"),
		tenants:            make(map[string]*Tenant),
	}

	idsource, err := authenv.ParseEnvironmentSource(config.IDSource)
	if err != nil {
		return nil, err
	}
	p.idSource = idsource

	if config.Statistics.Phase == "presample" {
		p.pbPhase = chqpb.Phase_PRE
	} else {
		p.pbPhase = chqpb.Phase_POST
	}

	processorId := set.ID.String()

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
	p.statsBatchSize = histogram

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
	p.cacheFull = cacheFullCounter

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
	p.recordLatency = recordLatencyHistogram

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

	ext, found := host.GetExtensions()[*p.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + p.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + p.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	p.configExtension = cext
	p.configCallbackID = p.configExtension.RegisterCallback(p.id.String()+"/"+p.ttype, p.configUpdateCallback)

	return nil
}

func (p *statsProcessor) Shutdown(ctx context.Context) error {
	var errors *multierror.Error
	p.configExtension.UnregisterCallback(p.configCallbackID)
	return errors.ErrorOrNil()
}

func (p *statsProcessor) processEnrichments(organizationID string, attributesByScope map[string]pcommon.Map) []*chqpb.Attribute {
	config := p.loadedConfig.Load()
	if config == nil {
		return nil
	}

	tenant := config.Configs[organizationID]
	if tenant.Stats == nil {
		return nil
	}

	stats := tenant.Stats[p.id.Name()]
	if stats == nil {
		return nil
	}

	tags := make([]*chqpb.Attribute, 0)
	var enrichments []ottl.StatsEnrichment
	switch p.ttype {
	case "logs":
		enrichments = stats.LogEnrichments
	case "metrics":
		enrichments = stats.MetricEnrichments
	case "traces":
		enrichments = stats.SpanEnrichments
	}

	for _, enrichment := range enrichments {
		attributes, found := attributesByScope[enrichment.Context]
		if found {
			for _, tag := range enrichment.Tags {
				if tagValue, found := attributes.Get(tag); found {
					tags = append(tags, toAttribute(enrichment.Context, tag, tagValue, false))
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

func (p *statsProcessor) configUpdateCallback(cpc ottl.ControlPlaneConfig) {
	p.logger.Info("Updating configuration for processor instance", zap.String("instance", p.id.Name()))
	p.loadedConfig.Store(&cpc)
}

func OrgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func CollectorIdFromResource(resource pcommon.Map) string {
	collectorId, found := resource.Get(translate.CardinalFieldCollectorID)
	if !found {
		return "default"
	}
	return collectorId.AsString()
}

func getFromResource(rl pcommon.Resource, key string) string {
	resourceAttributes := rl.Attributes()
	clusterVal, clusterFound := resourceAttributes.Get(key)
	cluster := clusterVal.AsString()
	if !clusterFound {
		cluster = "unknown"
	}
	return cluster
}

func computeExemplarKey(rl pcommon.Resource, extraKeys []string) ([]string, int64) {
	keys := []string{
		ClusterNameKey, getFromResource(rl, ServiceNameKey),
		NamespaceNameKey, getFromResource(rl, NamespaceNameKey),
		ServiceNameKey, getFromResource(rl, ServiceNameKey),
	}
	keys = append(keys, extraKeys...)
	return keys, hashString(keys)
}
