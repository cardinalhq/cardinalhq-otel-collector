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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/oteltools/pkg/ottl"
)

type fingerprintProcessor struct {
	config *Config
	logger *zap.Logger

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int
	logMappings      *MapStore
	logMappingsHash  int64

	// for logs
	logFingerprinter fingerprinter.Fingerprinter

	estimators          map[uint64]*SlidingEstimatorStat
	estimatorWindowSize int
	estimatorInterval   int64

	idSource authenv.EnvironmentSource
}

func newProcessor(config *Config, ttype string, set processor.Settings) (*fingerprintProcessor, error) {
	p := &fingerprintProcessor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
		logMappings:       NewMapStore(),
	}

	switch ttype {
	case "logs":
		p.logFingerprinter = fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30))

	case "traces":
		p.estimators = make(map[uint64]*SlidingEstimatorStat)
		p.estimatorWindowSize = config.TracesConfig.EstimatorWindowSize
		p.estimatorInterval = config.TracesConfig.EstimatorInterval
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
		//return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return nil
		//return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " is not a chqconfig extension")
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
		newhash := calculateMapHash(sc.FingerprintConfig.LogMappings)
		if newhash != p.logMappingsHash {
			p.logMappingsHash = newhash
			newMap := makeFingerprintMap(sc.FingerprintConfig.LogMappings)
			p.logMappings.Replace(newMap)
		}
		// Add span fingerprinter if needed
	}
	p.logger.Info("Configuration updated for processor instance", zap.String("instance", p.id.Name()))
}

// calculateMapHash calculates a hash of the fingerprint mappings used to detect
// a change.  It is not cryptographically secure.  An empty input list does not
// return a 0 value.
func calculateMapHash(m []ottl.FingerprintMapping) int64 {
	slices.SortStableFunc(m, func(i, j ottl.FingerprintMapping) int {
		if i.Primary < j.Primary {
			return -1
		}
		if i.Primary > j.Primary {
			return 1
		}
		return 0
	})

	hasher := fnv.New64a()
	buff := make([]byte, 8)

	for _, v := range m {
		binary.LittleEndian.PutUint64(buff, uint64(v.Primary))
		hasher.Write(buff)
		slices.Sort(v.Aliases)
		for _, vv := range v.Aliases {
			binary.LittleEndian.PutUint64(buff, uint64(vv))
			hasher.Write(buff)
		}
	}
	return int64(hasher.Sum64())
}

func makeFingerprintMap(m []ottl.FingerprintMapping) map[int64]int64 {
	ret := map[int64]int64{}
	for _, v := range m {
		for _, vv := range v.Aliases {
			ret[vv] = v.Primary
		}
	}
	return ret
}
