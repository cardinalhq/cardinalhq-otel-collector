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

package chqenforcerprocessor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqenforcerprocessor/internal/metadata"
	"github.com/hashicorp/go-multierror"
)

type chqEnforcer struct {
	config             *Config
	httpClient         *http.Client
	logger             *zap.Logger
	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	configExtension    *chqconfigextension.CHQConfigExtension
	configCallbackID   int
	pbPhase            chqpb.Phase
	podName            string
	vendor             string

	// for logs
	logstats    *stats.StatsCombiner[*chqpb.LogStats]
	logSampler  sampler.LogSampler
	spanSampler sampler.SpanSampler

	// for spans
	spanStats *stats.StatsCombiner[*chqpb.SpanStats]

	// for metrics
	metricstats          *stats.StatsCombiner[*MetricStat]
	nextMetricReceiver   consumer.Metrics
	aggregationInterval  time.Duration
	aggregatorI          sampler.MetricAggregator[int64]
	aggregatorF          sampler.MetricAggregator[float64]
	lastEmitCheck        time.Time
	aggregatedDatapoints metric.Int64Counter

	// for traces
	estimatorWindowSize  int
	estimatorInterval    int64
	estimators           map[uint64]*SlidingEstimatorStat
	estimatorLock        sync.Mutex
	slowSampler          sampler.Sampler
	hasErrorSampler      sampler.Sampler
	uninterestingSampler sampler.Sampler
	sentFingerprints     *fingerprintTracker
}

func newCHQEnforcer(config *Config, ttype string, set processor.Settings, nextConsumer consumer.Metrics) (*chqEnforcer, error) {
	now := time.Now()
	statsExporter := &chqEnforcer{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.Statistics.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		nextMetricReceiver: nextConsumer,
		podName:            os.Getenv("POD_NAME"),
		vendor:             config.Statistics.Vendor,
	}
	if config.Statistics.Phase == "presample" {
		statsExporter.pbPhase = chqpb.Phase_PRE
	} else {
		statsExporter.pbPhase = chqpb.Phase_POST
	}
	switch ttype {
	case "logs":
		statsExporter.logstats = stats.NewStatsCombiner[*chqpb.LogStats](now, config.Statistics.Interval)
		statsExporter.logger.Info("sending log statistics", zap.Duration("interval", config.Statistics.Interval))
		statsExporter.logSampler = sampler.NewLogSamplerImpl(context.Background(), statsExporter.logger)
	case "metrics":
		statsExporter.metricstats = stats.NewStatsCombiner[*MetricStat](now, config.Statistics.Interval)
		statsExporter.logger.Info("sending metric statistics", zap.Duration("interval", config.Statistics.Interval))
		statsExporter.lastEmitCheck = time.Now()
		interval := config.MetricAggregation.Interval.Milliseconds()
		statsExporter.aggregatorI = sampler.NewMetricAggregatorImpl[int64](interval, nil)
		statsExporter.aggregatorF = sampler.NewMetricAggregatorImpl[float64](interval, nil)
		err := statsExporter.setupMetricTelemetry()
		if err != nil {
			return nil, err
		}
	case "traces":
		statsExporter.estimators = make(map[uint64]*SlidingEstimatorStat)
		statsExporter.spanStats = stats.NewStatsCombiner[*chqpb.SpanStats](now, config.Statistics.Interval)
		statsExporter.logger.Info("sending span statistics", zap.Duration("interval", config.Statistics.Interval))
		statsExporter.spanSampler = sampler.NewSpanSamplerImpl(context.Background(), statsExporter.logger)
	}

	return statsExporter, nil
}

func (e *chqEnforcer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (e *chqEnforcer) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

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

	if e.ttype == "traces" {
		if err := e.slowSampler.Start(); err != nil {
			return fmt.Errorf("error starting slow sampler: %w", err)
		}
		if err := e.hasErrorSampler.Start(); err != nil {
			return fmt.Errorf("error starting has error sampler: %w", err)
		}
		if err := e.uninterestingSampler.Start(); err != nil {
			return fmt.Errorf("error starting uninteresting sampler: %w", err)
		}
		e.sentFingerprints = newFingerprintTracker()
	}
	return nil
}

func (e *chqEnforcer) Shutdown(ctx context.Context) error {
	var errors *multierror.Error
	e.configExtension.UnregisterCallback(e.configCallbackID)
	if e.slowSampler != nil {
		errors = multierror.Append(errors, e.slowSampler.Stop())
	}
	if e.hasErrorSampler != nil {
		errors = multierror.Append(errors, e.hasErrorSampler.Stop())
	}
	if e.uninterestingSampler != nil {
		errors = multierror.Append(errors, e.uninterestingSampler.Stop())
	}
	return errors.ErrorOrNil()
}

func (e *chqEnforcer) setupMetricTelemetry() error {
	m, err := metadata.Meter(e.telemetrySettings).Int64Counter("cardinalhq.enforcer.aggregated_datapoints",
		metric.WithDescription("Number of aggregated datapoints"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return err
	}
	e.aggregatedDatapoints = m
	return nil
}

func (e *chqEnforcer) configUpdateCallback(sc sampler.SamplerConfig) {
	switch e.ttype {
	case "logs":
		e.updateLogsamplingConfig(sc)
	case "metrics":
		e.updateMetricsamplingConfig(sc)
	}
	e.logger.Info("Configuration updated")
}
