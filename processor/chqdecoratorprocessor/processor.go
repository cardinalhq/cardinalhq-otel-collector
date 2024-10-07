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
	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"sync"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
	"net/http"
	"os"
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
	vendor             string

	logFingerprinter       fingerprinter.Fingerprinter
	logSampler             sampler.EventSampler
	logTransformations     ottl.Transformations
	metricsTransformations ottl.Transformations
	traceTransformations   ottl.Transformations
	traceSampler           sampler.EventSampler
	traceFingerprinter     fingerprinter.Fingerprinter

	// estimators for spans
	estimators map[uint64]*SlidingEstimatorStat

	estimatorWindowSize int
	estimatorInterval   int64
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
		vendor:             config.Statistics.Vendor,
	}

	switch ttype {
	case "logs":
		decorator.logger.Info("sending log statistics", zap.Duration("interval", config.Statistics.Interval))
		// Config Validation has already happened in Config.Validate, so we can safely ignore the error
		decorator.logFingerprinter = fingerprinter.NewFingerprinter()
		decorator.logTransformations = ottl.Transformations{}
		decorator.logSampler = sampler.NewEventSamplerImpl(context.Background(), decorator.logger)

	case "traces":
		decorator.logger.Info("sending span statistics", zap.Duration("interval", config.Statistics.Interval))
		decorator.traceFingerprinter = fingerprinter.NewFingerprinter()
		decorator.traceSampler = sampler.NewEventSamplerImpl(context.Background(), decorator.logger)
		decorator.estimators = make(map[uint64]*SlidingEstimatorStat)
		decorator.traceTransformations = ottl.Transformations{}
		decorator.estimatorWindowSize = config.TracesConfig.EstimatorWindowSize
		decorator.estimatorInterval = config.TracesConfig.EstimatorInterval

	case "metrics":
		decorator.metricsTransformations = ottl.Transformations{}

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
	var errors *multierror.Error
	c.configExtension.UnregisterCallback(c.configCallbackID)
	return errors.ErrorOrNil()
}

func (c *chqDecorator) configUpdateCallback(sc sampler.SamplerConfig) {
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
