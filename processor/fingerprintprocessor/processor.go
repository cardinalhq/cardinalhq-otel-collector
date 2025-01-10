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
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
)

type fingerprintProcessor struct {
	config *Config
	logger *zap.Logger

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings

	// for logs
	logFingerprinter fingerprinter.Fingerprinter

	// for spans
	traceFingerprinter  fingerprinter.Fingerprinter
	estimators          map[uint64]*SlidingEstimatorStat
	estimatorWindowSize int
	estimatorInterval   int64
}

func newPitbull(config *Config, ttype string, set processor.Settings) (*fingerprintProcessor, error) {
	dog := &fingerprintProcessor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
	}

	switch ttype {
	case "logs":
		dog.logFingerprinter = fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30))

	case "traces":
		dog.traceFingerprinter = fingerprinter.NewFingerprinter(fingerprinter.WithMaxTokens(30))
		dog.estimators = make(map[uint64]*SlidingEstimatorStat)
		dog.estimatorWindowSize = config.TracesConfig.EstimatorWindowSize
		dog.estimatorInterval = config.TracesConfig.EstimatorInterval
	}

	return dog, nil
}

func (e *fingerprintProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func ToMap(attributes pcommon.Map) map[string]string {
	result := make(map[string]string)
	attributes.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}
