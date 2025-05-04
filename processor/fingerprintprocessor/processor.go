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

package fingerprintprocessor

import (
	"context"
	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type fingerprintProcessor struct {
	config *Config
	logger *zap.Logger

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

	fingerprinters      syncmap.SyncMap[string, fingerprinter.Fingerprinter]
	lastTrieUpdateTimes syncmap.SyncMap[string, int64]

	idSource authenv.EnvironmentSource
}

func newProcessor(config *Config, ttype string, set processor.Settings) (*fingerprintProcessor, error) {
	p := &fingerprintProcessor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
	}

	idsource, err := authenv.ParseEnvironmentSource(config.IDSource)
	if err != nil {
		return nil, err
	}
	p.idSource = idsource

	return p, nil
}

func (p *fingerprintProcessor) GetOrCreateFingerprinter(cid string) fingerprinter.Fingerprinter {
	fpr, found := p.fingerprinters.Load(cid)
	if !found {
		clusterManager := fingerprinter.NewTrieClusterManager(0.5)
		fpr = fingerprinter.NewFingerprinter(clusterManager)
		p.fingerprinters.Store(cid, fpr)
	}
	return fpr
}

func (p *fingerprintProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *fingerprintProcessor) Start(ctx context.Context, host component.Host) error {
	ext, found := host.GetExtensions()[*p.config.ConfigurationExtension]
	if !found {
		return nil
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return nil
	}
	p.configExtension = cext

	p.configCallbackID = p.configExtension.RegisterCallback(p.id.String()+"/"+p.ttype, p.configUpdateCallback)

	return nil
}

func (p *fingerprintProcessor) Shutdown(ctx context.Context) error {
	p.configExtension.UnregisterCallback(p.configCallbackID)
	return nil
}

func (p *fingerprintProcessor) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	for cid, v := range sc.Configs {
		trie := v.FingerprintConfig.Trie
		trieUpdateTime := v.FingerprintConfig.LastUpdateTime
		fpr := p.GetOrCreateFingerprinter(cid)
		lastUpdateTime, loaded := p.lastTrieUpdateTimes.Load(cid)
		if !loaded || trieUpdateTime > lastUpdateTime {
			err := fpr.GetClusterManager().Restore(trie)
			p.lastTrieUpdateTimes.Store(cid, trieUpdateTime)
			if err != nil {
				p.logger.Error("Error restoring trie", zap.Error(err))
				continue
			}
		}
	}
	p.logger.Info("Configuration updated for processor instance", zap.String("instance", p.id.Name()))
}

func OrgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}
