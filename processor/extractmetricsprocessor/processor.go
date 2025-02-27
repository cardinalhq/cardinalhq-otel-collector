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

package extractmetricsprocessor

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/extractmetricsprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/otel/metric"

	"github.com/observiq/bindplane-otel-collector/receiver/routereceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/ottl"
)

type extractor struct {
	config          *Config
	logger          *zap.Logger
	configExtension *chqconfigextension.CHQConfigExtension

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings
	rulesEvaluated    telemetry.DeferrableCounter[int64]
	rulesExecuted     telemetry.DeferrableCounter[int64]
	ruleErrors        telemetry.DeferrableCounter[int64]
	ruleEvalTime      telemetry.DeferrableHistogram[int64]

	configCallbackID int
	logExtractors    syncmap.SyncMap[string, []*ottl.LogExtractor]
	spanExtractors   syncmap.SyncMap[string, []*ottl.SpanExtractor]
}

func newExtractor(config *Config, ttype string, set processor.Settings) (*extractor, error) {
	p := &extractor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
	}

	counter, counterError := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"ottl.extract_rules.evaluated.count",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of rule conditions evaluated"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{},
	)
	if counterError != nil {
		return nil, counterError
	}
	p.rulesEvaluated = counter

	counter, counterError = telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"ottl.extract_rules.executed.count",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of rules executed"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{},
	)
	if counterError != nil {
		return nil, counterError
	}
	p.rulesExecuted = counter

	errorCounter, errorCounterError := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"ottl.extract_rules.error.count",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of errors encountered while evaluating rules"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{},
	)
	if errorCounterError != nil {
		return nil, counterError
	}
	p.ruleErrors = errorCounter

	histogram, histogramError := telemetry.NewDeferrableHistogram(metadata.Meter(set.TelemetrySettings),
		"ottl.extract_rules.processing.time",
		[]metric.Int64HistogramOption{
			metric.WithDescription("The time taken to evaluate rules"),
			metric.WithUnit("ns"),
		},
		[]metric.RecordOption{},
	)
	if histogramError != nil {
		return nil, histogramError
	}
	p.ruleEvalTime = histogram

	return p, nil
}

func (p *extractor) Start(ctx context.Context, host component.Host) error {
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

func (p *extractor) Shutdown(ctx context.Context) error {
	p.configExtension.UnregisterCallback(p.configCallbackID)
	return nil
}

func (p *extractor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func convertAnyToInt(value any) (int64, error) {
	switch value := value.(type) {
	case int:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case float32:
		return int64(value), nil
	case float64:
		return int64(value), nil
	case string:
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("failed to convert string to int: %s", value)
	default:
		return 0, fmt.Errorf("invalid value type: %T", value)
	}
}

func convertAnyToFloat(value any) (float64, error) {
	switch value := value.(type) {
	case int:
		return float64(value), nil
	case int32:
		return float64(value), nil
	case int64:
		return float64(value), nil
	case float32:
		return float64(value), nil
	case float64:
		return value, nil
	case string:
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("failed to convert string to float: %s", value)
	default:
		return 0, fmt.Errorf("invalid value type: %T", value)
	}
}

// sendMetrics sends metrics to the configured route.
func (p *extractor) sendMetrics(ctx context.Context, route string, metrics pmetric.Metrics) {
	err := routereceiver.RouteMetrics(ctx, route, metrics)
	if err != nil {
		p.logger.Error("Failed to send metrics", zap.Error(err))
	}
}

func (p *extractor) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	for cid, tenant := range sc.Configs {
		p.updateForTenant(cid, tenant)
	}
}

func (p *extractor) updateForTenant(cid string, sc ottl.TenantConfig) {
	configs, found := sc.ExtractMetrics[p.id.Name()]
	if !found || configs == nil {
		p.logExtractors.Delete(cid)
		p.spanExtractors.Delete(cid)
		return
	}

	switch p.ttype {
	case "logs":
		parsedExtractors, err := ottl.ParseLogExtractorConfigs(configs.LogMetricExtractors, p.logger)
		p.logger.Info("Setting log extractors", zap.String("id", p.id.Name()), zap.Int("num_configs", len(configs.LogMetricExtractors)), zap.Int("num_parsed_configs", len(parsedExtractors)))
		if err != nil {
			p.logger.Error("Error parsing log extractor configurations", zap.Error(err))
			return
		}
		p.logExtractors.Store(cid, parsedExtractors)

	case "traces":
		parsedExtractors, err := ottl.ParseSpanExtractorConfigs(configs.SpanMetricExtractors, p.logger)
		if err != nil {
			p.logger.Error("Error parsing log extractor configurations", zap.Error(err))
			return
		}
		p.spanExtractors.Store(cid, parsedExtractors)

	default: // ignore
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
