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

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
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

	trieClusterManagers syncmap.SyncMap[string, *fingerprinter.TrieClusterManager]
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

	if config.ConfigurationExtension != nil {
		p.logger.Warn("Ignoring deprecated configuration_extension field", zap.String("id", config.ConfigurationExtension.String()))
	}

	idsource, err := authenv.ParseEnvironmentSource(config.IDSource)
	if err != nil {
		return nil, err
	}
	p.idSource = idsource

	return p, nil
}

func (p *fingerprintProcessor) GetOrCreateTrieClusterManager(cid string) *fingerprinter.TrieClusterManager {
	clusterManager, found := p.trieClusterManagers.Load(cid)
	if !found {
		clusterManager = fingerprinter.NewTrieClusterManager(0.5)
		p.trieClusterManagers.Store(cid, clusterManager)
	}
	return clusterManager
}

func (p *fingerprintProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *fingerprintProcessor) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (p *fingerprintProcessor) Shutdown(ctx context.Context) error {
	return nil
}

func OrgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}
