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

	"github.com/observiq/bindplane-agent/receiver/routereceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
)

type extractor struct {
	config          *Config
	logger          *zap.Logger
	configExtension *chqconfigextension.CHQConfigExtension

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings

	configCallbackID int
	logExtractors    *[]ottl.LogExtractor
	spanExtractors   *[]ottl.SpanExtractor
}

func newExtractor(config *Config, ttype string, set processor.Settings) (*extractor, error) {
	e := &extractor{
		id:                set.ID,
		ttype:             ttype,
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.Logger,
	}

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

func (e *extractor) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	var extractorConfigs = make([]*ottl.LogExtractor, 0)

	if len(extractorConfigs) > 0 {
		switch e.ttype {
		case "logs":
			parsedExtractors, err := ottl.ParseLogExtractorConfigs(sc.LogMetricExtractors, e.logger)
			if err != nil {
				e.logger.Error("Error parsing log extractor configurations", zap.Error(err))
				return
			}
			e.logExtractors = ottl.ConvertToPointerArray(parsedExtractors)

		case "traces":
			parsedExtractors, err := ottl.ParseSpanExtractorConfigs(sc.SpanMetricExtractors, e.logger)
			if err != nil {
				e.logger.Error("Error parsing log extractor configurations", zap.Error(err))
				return
			}
			e.spanExtractors = ottl.ConvertToPointerArray(parsedExtractors)

		default:
		}
	}
	e.logger.Info("Configuration updated")
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
