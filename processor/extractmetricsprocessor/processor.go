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

package extractmetricsprocessor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/telemetry"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/extractmetricsprocessor/internal/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/observiq/bindplane-agent/receiver/routereceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/ottl"
)

type extractor struct {
	config          *Config
	logger          *zap.Logger
	configExtension *chqconfigextension.CHQConfigExtension

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings
	rulesEvaluated    *telemetry.DeferrableInt64Counter
	ruleErrors        *telemetry.DeferrableInt64Counter
	ruleEvalTime      *telemetry.DeferrableInt64Histogram

	configCallbackID int
	logExtractors    atomic.Pointer[[]*ottl.LogExtractor]
	spanExtractors   atomic.Pointer[[]*ottl.SpanExtractor]
}

func newExtractor(config *Config, ttype string, set processor.Settings) (*extractor, error) {
	e := &extractor{
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
			metric.WithDescription("The number of rules evaluated"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if counterError != nil {
		return nil, counterError
	}
	e.rulesEvaluated = counter

	errorCounter, errorCounterError := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"ottl_rule_eval_errors",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of rules evaluated"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if errorCounterError != nil {
		return nil, counterError
	}
	e.ruleErrors = errorCounter

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
	e.ruleEvalTime = histogram

	return e, nil
}

func (e *extractor) Start(ctx context.Context, host component.Host) error {
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

func (e *extractor) Shutdown(ctx context.Context) error {
	e.configExtension.UnregisterCallback(e.configCallbackID)
	return nil
}

func (e *extractor) Capabilities() consumer.Capabilities {
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
func (e *extractor) sendMetrics(ctx context.Context, route string, metrics pmetric.Metrics) {
	err := routereceiver.RouteMetrics(ctx, route, metrics)
	if err != nil {
		e.logger.Error("Failed to send metrics", zap.Error(err))
	}
}

func (e *extractor) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	configs := sc.ExtractMetrics[e.id.Name()]

	switch e.ttype {
	case "logs":
		parsedExtractors, err := ottl.ParseLogExtractorConfigs(configs.LogMetricExtractors, e.logger)
		e.logger.Info("Setting log extractors", zap.String("id", e.id.Name()), zap.Int("num_configs", len(configs.LogMetricExtractors)), zap.Int("num_parsed_configs", len(parsedExtractors)))
		if err != nil {
			e.logger.Error("Error parsing log extractor configurations", zap.Error(err))
			return
		}
		e.logExtractors.Store(&parsedExtractors)

	case "traces":
		parsedExtractors, err := ottl.ParseSpanExtractorConfigs(configs.SpanMetricExtractors, e.logger)
		if err != nil {
			e.logger.Error("Error parsing log extractor configurations", zap.Error(err))
			return
		}
		e.spanExtractors.Store(&parsedExtractors)

	default: // ignore
	}
	e.logger.Info("Configuration updated for processor instance", zap.String("instance", e.id.Name()))
}
