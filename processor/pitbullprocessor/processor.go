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

package pitbullprocessor

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/signalnames"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

type pitbull struct {
	config            *Config
	logger            *zap.Logger
	id                component.ID
	ttype             signalnames.Name
	telemetrySettings component.TelemetrySettings

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

	logTransformations    syncmap.SyncMap[string, *ottl.Transformations]
	logsLookupConfigs     syncmap.SyncMap[string, *[]ottl.LookupConfig]
	traceTransformations  syncmap.SyncMap[string, *ottl.Transformations]
	tracesLookupConfigs   syncmap.SyncMap[string, *[]ottl.LookupConfig]
	metricTransformations syncmap.SyncMap[string, *ottl.Transformations]
	metricsLookupConfigs  syncmap.SyncMap[string, *[]ottl.LookupConfig]

	ottlTelemetry *ottl.Telemetry
}

func newPitbull(config *Config, ttype signalnames.Name, set processor.Settings) (*pitbull, error) {
	return &pitbull{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
		ottlTelemetry:     ottl.NewTelemetry(metadata.Meter(set.TelemetrySettings)),
	}, nil
}

func (p *pitbull) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *pitbull) Start(_ context.Context, host component.Host) error {
	ext, found := host.GetExtensions()[*p.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + p.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + p.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	p.configExtension = cext

	p.configCallbackID = p.configExtension.RegisterCallback(p.id.String()+"/"+p.ttype.String(), p.configUpdateCallback)

	return nil
}

func (p *pitbull) Shutdown(_ context.Context) error {
	p.configExtension.UnregisterCallback(p.configCallbackID)
	return nil
}

func (p *pitbull) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	p.logger.Info("Configuration updated for processor instance", zap.String("instance", p.id.Name()))

	for cid, tenant := range sc.Configs {
		pbc := tenant.Pitbulls[p.id.Name()]
		p.logger.Info("Configuration updated for tenant", zap.String("instance", p.id.Name()), zap.String("tenant", cid), zap.Bool("config_present", pbc != nil))
		p.updateLogConfigForTenant(cid, pbc)
		p.updateMetricConfigForTenant(cid, pbc)
		p.updateTraceConfigForTenant(cid, pbc)
	}
}

func (p *pitbull) shutdownLogsForTenant(cid string) {
	if oldItems, found := p.logTransformations.Load(cid); found {
		p.logTransformations.Delete(cid)
		oldItems.Stop()
	}
	p.logsLookupConfigs.Delete(cid)
}

func (p *pitbull) shutdownMetricsForTenant(cid string) {
	if oldItems, found := p.metricTransformations.Load(cid); found {
		p.metricTransformations.Delete(cid)
		oldItems.Stop()
	}
	p.metricsLookupConfigs.Delete(cid)
}

func (p *pitbull) shutdownTraceForTenant(cid string) {
	if oldItems, found := p.traceTransformations.Load(cid); found {
		p.traceTransformations.Delete(cid)
		oldItems.Stop()
	}
	p.tracesLookupConfigs.Delete(cid)
}

func orgIDFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func (p *pitbull) attributesFor(cid string) attribute.Set {
	return attribute.NewSet(
		attribute.String("processor", p.id.String()),
		attribute.String("signal", p.ttype.String()),
		attribute.String("organization_id", cid),
	)
}
