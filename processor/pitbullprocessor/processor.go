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

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

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
	p := &pitbull{
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
	p.ottlProcessed = counter

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
	p.ottlErrors = errorCounter

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
	p.histogram = histogram

	return p, nil
}

func (p *pitbull) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *pitbull) Start(ctx context.Context, host component.Host) error {
	ext, found := host.GetExtensions()[*p.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + p.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + p.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	p.configExtension = cext

	p.configCallbackID = p.configExtension.RegisterCallback(p.id.String()+"/"+p.ttype, p.configUpdateCallback)

	return nil
}

func (p *pitbull) Shutdown(ctx context.Context) error {
	p.configExtension.UnregisterCallback(p.configCallbackID)
	return nil
}

func (p *pitbull) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	p.logger.Info("Got configuration update for processor instance", zap.String("instance", p.id.Name()), zap.Int("pitbullConfigs", len(sc.Pitbulls)))
	configs := sc.Pitbulls[p.id.Name()]
	p.logger.Info("Updating configuration for processor instance",
		zap.String("instance", p.id.Name()),
		zap.Int("logLookupConfigs", len(configs.LogLookupConfigs)),
		zap.Int("logStatements", len(configs.LogStatements)),
		zap.Int("spanLookupConfigs", len(configs.SpanLookupConfigs)),
		zap.Int("spanStatements", len(configs.SpanStatements)),
		zap.Int("metricLookupConfigs", len(configs.MetricLookupConfigs)),
		zap.Int("metricStatements", len(configs.MetricStatements)))

	switch p.ttype {
	case "logs":
		p.updateLogTransformations(configs, p.logger)
	case "traces":
		p.updateTraceTransformations(configs, p.logger)
	case "metrics":
		p.updateMetricTransformation(configs, p.logger)
	}
	p.logger.Info("Configuration updated for processor instance", zap.String("instance", p.id.Name()))
}
