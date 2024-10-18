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

package chqdecoratorprocessor

import (
	"context"
	"errors"
	"sync"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/metadata"

	"net/http"
	"os"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type chqDecorator struct {
	sync.RWMutex

	config             *Config
	httpClient         *http.Client
	logger             *zap.Logger
	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	configExtension    *chqconfigextension.CHQConfigExtension
	configCallbackID   int
	podName            string

	logFingerprinter fingerprinter.Fingerprinter

	logTransformations     ottl.Transformations
	metricsTransformations ottl.Transformations
	traceTransformations   ottl.Transformations

	traceFingerprinter fingerprinter.Fingerprinter

	// estimators for spans
	estimators          map[uint64]*SlidingEstimatorStat
	estimatorWindowSize int
	estimatorInterval   int64

	ottlProcessed *ottl.TransformCounter
}

func newCHQDecorator(config *Config, ttype string, set processor.Settings) (*chqDecorator, error) {
	decorator := &chqDecorator{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.Statistics.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		podName:            os.Getenv("POD_NAME"),
	}

	attrset := attribute.NewSet(
		attribute.String("processor", set.ID.String()),
		attribute.String("signal", ttype),
	)
	counter, err := ottl.NewTransformCounter(metadata.Meter(set.TelemetrySettings),
		"ottl_processed",
		[]metric.Int64CounterOption{
			metric.WithDescription("The results of OTTL processing"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if err != nil {
		return nil, err
	}
	decorator.ottlProcessed = counter

	switch ttype {
	case "logs":
		decorator.logger.Info("sending log statistics", zap.Duration("interval", config.Statistics.Interval))
		// Config Validation has already happened in Config.Validate, so we can safely ignore the error
		decorator.logFingerprinter = fingerprinter.NewFingerprinter()
		decorator.logTransformations = ottl.NewTransformations(set.Logger)

	case "traces":
		decorator.logger.Info("sending span statistics", zap.Duration("interval", config.Statistics.Interval))
		decorator.traceFingerprinter = fingerprinter.NewFingerprinter()
		decorator.estimators = make(map[uint64]*SlidingEstimatorStat)
		decorator.traceTransformations = ottl.NewTransformations(set.Logger)
		decorator.estimatorWindowSize = config.TracesConfig.EstimatorWindowSize
		decorator.estimatorInterval = config.TracesConfig.EstimatorInterval

	case "metrics":
		decorator.metricsTransformations = ottl.NewTransformations(set.Logger)

	default:
		return nil, errors.New("unknown decorator type " + ttype)
	}

	return decorator, nil
}

func (c *chqDecorator) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *chqDecorator) Start(ctx context.Context, host component.Host) error {
	httpClient, err := c.httpClientSettings.ToClient(ctx, host, c.telemetrySettings)
	if err != nil {
		return err
	}
	c.httpClient = httpClient

	if c.config.ConfigurationExtension == nil {
		c.logger.Warn("Configuration extension not set, no configuration for filtering will be enabled")
		return nil
	}

	ext, found := host.GetExtensions()[*c.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + c.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + c.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	c.configExtension = cext
	c.configCallbackID = c.configExtension.RegisterCallback(c.id.String()+"/"+c.ttype, c.configUpdateCallback)

	return nil
}

func (c *chqDecorator) Shutdown(ctx context.Context) error {
	if c.configExtension != nil {
		c.configExtension.UnregisterCallback(c.configCallbackID)
	}
	return nil
}

func (c *chqDecorator) configUpdateCallback(sc ottl.SamplerConfig) {
	switch c.ttype {
	case "logs":
		c.updateLogsSampling(sc)
	case "traces":
		c.updateTracesSampling(sc)
	case "metrics":
		c.updateMetricsTransformation(sc)
	}
	c.logger.Info("Configuration updated")
}
