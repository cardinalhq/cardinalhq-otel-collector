// Copyright 2024 CardinalHQ, Inc
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

package piiredactionprocessor

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/pii"
)

type piiRedactionProcessor struct {
	config *Config
	logger *zap.Logger

	id                component.ID
	telemetrySettings component.TelemetrySettings

	detector pii.Detector
}

func newProcessor(config *Config, set processor.Settings) (*piiRedactionProcessor, error) {
	dog := &piiRedactionProcessor{
		id:                set.ID,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
	}

	types := []pii.PIIType{
		pii.PIITypeEmail,
		pii.PIITypePhone,
		pii.PIITypeSSN,
		pii.PIITypeCCN,
	}

	if len(config.Detectors) > 0 {
		types = nil
		for _, detector := range config.Detectors {
			t, err := mapDetectorNameToType(detector)
			if err != nil {
				return nil, err
			}
			types = append(types, t)
		}
	}

	dog.detector = pii.NewDetector(pii.WithPIITypes(types...))
	return dog, nil
}

func (e *piiRedactionProcessor) Capabilities() consumer.Capabilities {
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
