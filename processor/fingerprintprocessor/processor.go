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
	"encoding/binary"
	"hash/fnv"
	"slices"
	"strconv"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

type fingerprintProcessor struct {
	config *Config
	logger *zap.Logger

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

	logMappingsHash int64

	// for logs
	logFingerprinter   fingerprinter.Fingerprinter
	traceFingerprinter fingerprinter.Fingerprinter

	tenants syncmap.SyncMap[string, *tenantState]

	estimatorWindowSize int
	estimatorInterval   int64

	idSource authenv.EnvironmentSource
}

type tenantState struct {
	mapstore   *MapStore
	estimators syncmap.SyncMap[uint64, *SlidingEstimatorStat]
}

func newProcessor(config *Config, ttype string, set processor.Settings) (*fingerprintProcessor, error) {
	p := &fingerprintProcessor{
		id:                  set.ID,
		ttype:               ttype,
		config:              config,
		telemetrySettings:   set.TelemetrySettings,
		logger:              set.Logger,
		estimatorWindowSize: config.TracesConfig.EstimatorWindowSize,
		estimatorInterval:   config.TracesConfig.EstimatorInterval,
	}

	switch ttype {
	case "logs":
		p.logFingerprinter = fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30))
	case "traces":
		p.traceFingerprinter = fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30))
	}

	idsource, err := authenv.ParseEnvironmentSource(config.IDSource)
	if err != nil {
		return nil, err
	}
	p.idSource = idsource

	return p, nil
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
	switch p.ttype {
	case "logs":
		newhash := calculateMapHash(sc)
		if newhash != p.logMappingsHash {
			p.logMappingsHash = newhash
			newMappings := makeFingerprintMap(sc)
			for cid, v := range newMappings {
				p.logger.Info("Configuration updated for tenant", zap.String("instance", p.id.Name()), zap.String("tenant", cid), zap.Int("mappingsCount", len(v)))
				tenant := p.getTenantUnlocked(cid)
				tenant.mapstore.Replace(v)
			}
		}
		// Add span fingerprinter if needed
	}
	p.logger.Info("Configuration updated for processor instance", zap.String("instance", p.id.Name()))
}

// calculateMapHash calculates a hash of the fingerprint mappings used to detect
// a change.  It is not cryptographically secure.  An empty input list does not
// return a 0 value.
func calculateMapHash(m ottl.ControlPlaneConfig) int64 {
	hasher := fnv.New64a()

	cids := make([]string, 0, len(m.Configs))
	for cid := range m.Configs {
		cids = append(cids, cid)
	}
	slices.Sort(cids)

	for _, cid := range cids {
		v := m.Configs[cid]
		_, _ = hasher.Write([]byte(cid))
		fpm := v.FingerprintConfig.LogMappings
		slices.SortStableFunc(fpm, func(i, j ottl.FingerprintMapping) int {
			if i.Primary < j.Primary {
				return -1
			}
			if i.Primary > j.Primary {
				return 1
			}
			return 0
		})

		buff := make([]byte, 8)
		for _, v := range fpm {
			primary, err := strconv.ParseInt(v.Primary, 10, 64)
			if err != nil {
				continue
			}
			binary.LittleEndian.PutUint64(buff, uint64(primary))
			hasher.Write(buff)
			slices.Sort(v.Aliases)
			for _, vv := range v.Aliases {
				alias, err := strconv.ParseInt(vv, 10, 64)
				if err != nil {
					continue
				}
				binary.LittleEndian.PutUint64(buff, uint64(alias))
				hasher.Write(buff)
			}
		}
	}

	return int64(hasher.Sum64())
}

func makeFingerprintMap(m ottl.ControlPlaneConfig) map[string]map[int64]int64 {
	ret := map[string]map[int64]int64{}
	for cid, v := range m.Configs {
		ret[cid] = makeFingerprintMapForConfig(v.FingerprintConfig.LogMappings)
	}
	return ret
}

func makeFingerprintMapForConfig(m []ottl.FingerprintMapping) map[int64]int64 {
	ret := map[int64]int64{}
	for _, v := range m {
		for _, vv := range v.Aliases {
			primary, err := strconv.ParseInt(v.Primary, 10, 64)
			if err != nil {
				continue
			}
			alias, err := strconv.ParseInt(vv, 10, 64)
			if err != nil {
				continue
			}
			ret[alias] = primary
		}
	}
	return ret
}

func OrgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func (p *fingerprintProcessor) getTenant(cid string) *tenantState {
	return p.getTenantUnlocked(cid)
}

func (p *fingerprintProcessor) getTenantUnlocked(cid string) *tenantState {
	tenant, found := p.tenants.Load(cid)
	if !found {
		tenant = &tenantState{
			mapstore: NewMapStore(),
		}
		p.tenants.Store(cid, tenant)
	}

	return tenant
}
