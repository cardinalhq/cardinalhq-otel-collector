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

package piiredactionprocessor

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/piiredactionprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/pii"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
)

type piiRedactionProcessor struct {
	config *Config
	logger *zap.Logger

	id                component.ID
	telemetrySettings component.TelemetrySettings

	detector   pii.Detector
	detections telemetry.DeferrableCounter
	redactions telemetry.DeferrableCounter
}

func newProcessor(config *Config, set processor.Settings) (*piiRedactionProcessor, error) {
	p := &piiRedactionProcessor{
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

	p.detector = pii.NewDetector(
		pii.WithPIITypes(types...),
		pii.WithDetectionStats(p.detections),
		pii.WithRedactionStats(p.redactions),
	)

	attrset := attribute.NewSet(
		attribute.String("processor", set.ID.String()),
		attribute.String("signal", "logs"),
	)

	detections, err := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"otelcol_processor_pii_redaction_detections",
		[]metric.Int64CounterOption{},
		[]metric.AddOption{metric.WithAttributeSet(attrset)},
	)
	if err != nil {
		return nil, err
	}
	p.detections = detections

	redactions, err := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"otelcol_processor_pii_redaction_redactions",
		[]metric.Int64CounterOption{},
		[]metric.AddOption{metric.WithAttributeSet(attrset)},
	)
	if err != nil {
		return nil, err
	}
	p.redactions = redactions

	return p, nil
}

func (p *piiRedactionProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
