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
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"net/http"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/signalnames"
	"github.com/cardinalhq/oteltools/hashutils"
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

// exemplarProcessor is a processor that sends exemplars to a remote service.
// This is not a shared component, and so we can only initialize the
// parts we need here and dedicate it to the specific telemetry type.
type exemplarProcessor struct {
	config             *Config
	httpClient         *http.Client
	logger             *zap.Logger
	id                 component.ID
	ttype              signalnames.Name
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	tenants          syncmap.SyncMap[string, *Tenant]
	serviceLogCounts syncmap.SyncMap[string, *chqpb.ServiceLogCountsCache]
}

// Tenant holds the caches for each telemetry type, although for any given
// processor instance, only one of these will be used.
type Tenant struct {
	logCache    *LRUCache[plog.Logs]
	metricCache *LRUCache[pmetric.Metrics]
	traceCache  *LRUCache[ptrace.Traces]
}

func (p *exemplarProcessor) getTenant(organizationID string) *Tenant {
	tenant, _, _ := p.tenants.LoadOrStoreFunc(organizationID, func() (*Tenant, error) {
		p.logger.Info("Creating new tenant", zap.String("organization_id", organizationID))
		tenant := &Tenant{}
		switch p.ttype {
		case signalnames.Logs:
			tenant.logCache = NewLRUCache(
				p.config.Reporting.Logs.CacheSize,
				p.config.Reporting.Logs.Expiry,
				p.config.Reporting.Logs.Interval,
				sendExemplars[plog.Logs](p, organizationID, p.id.Name()))
		case signalnames.Metrics:
			tenant.metricCache = NewLRUCache(
				p.config.Reporting.Metrics.CacheSize,
				p.config.Reporting.Metrics.Expiry,
				p.config.Reporting.Metrics.Interval,
				sendExemplars[pmetric.Metrics](p, organizationID, p.id.Name()))
		case signalnames.Traces:
			tenant.traceCache = NewLRUCache(
				p.config.Reporting.Traces.CacheSize,
				p.config.Reporting.Traces.Expiry,
				p.config.Reporting.Traces.Interval,
				sendExemplars[ptrace.Traces](p, organizationID, p.id.Name()))
		case signalnames.Profiles:
			// Do nothing
		default:
			// Do nothing
		}
		return tenant, nil
	})
	return tenant
}

func newProcessor(config *Config, ttype signalnames.Name, set processor.Settings) (*exemplarProcessor, error) {
	p := &exemplarProcessor{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
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

func getFromResource(attr pcommon.Map, key string) string {
	clusterVal, clusterFound := attr.Get(key)
	if !clusterFound {
		return "unknown"
	}
	return clusterVal.AsString()
}

func computeExemplarKey(rl pcommon.Resource, extraKeys []string) ([]string, int64) {
	keys := []string{
		clusterNameKey, getFromResource(rl.Attributes(), clusterNameKey),
		namespaceNameKey, getFromResource(rl.Attributes(), namespaceNameKey),
		serviceNameKey, getFromResource(rl.Attributes(), serviceNameKey),
	}
	keys = append(keys, extraKeys...)
	return keys, int64(hashutils.HashStrings(nil, keys...))
}
