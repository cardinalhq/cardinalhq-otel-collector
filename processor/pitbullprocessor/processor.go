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

package pitbullprocessor

import (
	"context"
	"errors"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
)

type pitbull struct {
	config            *Config
	logger            *zap.Logger
	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings
	configExtension   *chqconfigextension.CHQConfigExtension

	configCallbackID      int
	logTransformations    atomic.Pointer[ottl.Transformations]
	logsLookupConfigs     atomic.Pointer[[]ottl.LookupConfig]
	traceTransformations  atomic.Pointer[ottl.Transformations]
	tracesLookupConfigs   atomic.Pointer[[]ottl.LookupConfig]
	metricTransformations atomic.Pointer[ottl.Transformations]
	metricsLookupConfigs  atomic.Pointer[[]ottl.LookupConfig]
	ottlProcessed         *telemetry.DeferrableInt64Counter
	ottlErrors            *telemetry.DeferrableInt64Counter
	histogram             *telemetry.DeferrableInt64Histogram
}

func newPitbull(config *Config, ttype string, set processor.Settings) (*pitbull, error) {
	dog := &pitbull{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
	}

	attrset := attribute.NewSet(
		attribute.String("processor", set.ID.String()),
		attribute.String("signal", ttype),
	)
	counter, counterError := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"ottl_rules_processed",
		[]metric.Int64CounterOption{
			metric.WithDescription("The results of OTTL processing"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if counterError != nil {
		return nil, counterError
	}
	dog.ottlProcessed = counter

	errorCounter, errCounterError := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"ottl_rule_eval_errors",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of errors encountered during OTTL processing"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if errCounterError != nil {
		return nil, errCounterError
	}
	dog.ottlErrors = errorCounter

	histogram, histogramError := telemetry.NewDeferrableHistogram(metadata.Meter(set.TelemetrySettings),
		"ottl_rule_eval_time",
		[]metric.Int64HistogramOption{},
		[]metric.RecordOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if histogramError != nil {
		return nil, histogramError
	}
	dog.histogram = histogram

	return dog, nil
}

func (e *pitbull) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (e *pitbull) Start(ctx context.Context, host component.Host) error {
	ext, found := host.GetExtensions()[*e.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	e.configExtension = cext

	e.configCallbackID = e.configExtension.RegisterCallback(e.id.String()+"/"+e.ttype, e.configUpdateCallback)

	return nil
}

func (e *pitbull) Shutdown(ctx context.Context) error {
	e.configExtension.UnregisterCallback(e.configCallbackID)
	return nil
}

func (e *pitbull) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	configs := sc.Pitbulls[e.id.Name()]

	switch e.ttype {
	case "logs":
		e.updateLogTransformations(configs, e.logger)
	case "traces":
		e.updateTraceTransformations(configs, e.logger)
	case "metrics":
		e.updateMetricTransformation(configs, e.logger)
	}
	e.logger.Info("Configuration updated for processor instance", zap.String("instance", e.id.Name()))
}
