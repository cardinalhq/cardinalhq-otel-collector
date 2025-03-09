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

package chqexemplarprocessor

import (
	"context"
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/translate"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
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

type exemplarProcessor struct {
	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	tenants syncmap.SyncMap[string, *Tenant]

	jsonMarshaller otelJsonMarshaller
}

type Tenant struct {
	logExemplars    *LRUCache
	traceExemplars  *LRUCache
	metricExemplars *LRUCache
}

func (p *exemplarProcessor) getTenant(organizationID string) *Tenant {
	tenant, _ := p.tenants.LoadOrStore(organizationID, func() *Tenant {
		tenant := &Tenant{}
		switch p.ttype {
		case "logs":
			tenant.logExemplars = NewLRUCache(1000, 30*time.Minute, p.sendExemplars(organizationID, p.ttype, p.id.Name()))
		case "metrics":
			tenant.metricExemplars = NewLRUCache(1000, 30*time.Minute, p.sendExemplars(organizationID, p.ttype, p.id.Name()))
		case "traces":
			tenant.traceExemplars = NewLRUCache(1000, 30*time.Minute, p.sendExemplars(organizationID, p.ttype, p.id.Name()))
		}
		return tenant
	})

	return tenant
}

func newProcessor(config *Config, ttype string, set processor.Settings) (*exemplarProcessor, error) {
	p := &exemplarProcessor{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		jsonMarshaller:     newMarshaller(),
		logger:             set.Logger,
		tenants:            make(map[string]*Tenant),
	}

	return p, nil
}

func (p *exemplarProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *exemplarProcessor) Start(ctx context.Context, host component.Host) error {
	httpClient, err := p.httpClientSettings.ToClient(ctx, host, p.telemetrySettings)
	if err != nil {
		return err
	}
	p.httpClient = httpClient

	return nil
}

func orgIdFromResource(resource pcommon.Map) string {
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
