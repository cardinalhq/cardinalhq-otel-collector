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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"go.opentelemetry.io/collector/pdata/pcommon"
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
	sync.RWMutex

	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

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
	logstats           *stats.StatsCombiner[*chqpb.LogStats]
	logTransformations ottl.Transformations

	// for spans
	spanStats            *stats.StatsCombiner[*chqpb.SpanStats]
	traceTransformations ottl.Transformations

	// for metrics
	metricstats           *stats.StatsCombiner[*MetricStat]
	metricTransformations ottl.Transformations

	nextMetricReceiver   consumer.Metrics
	aggregationInterval  time.Duration
	aggregatorI          sampler.MetricAggregator[int64]
	aggregatorF          sampler.MetricAggregator[float64]
	lastEmitCheck        time.Time
	aggregatedDatapoints metric.Int64Counter
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
		statsExporter.logTransformations = ottl.Transformations{}

	case "metrics":
		statsExporter.metricstats = stats.NewStatsCombiner[*MetricStat](now, config.Statistics.Interval)
		statsExporter.logger.Info("sending metric statistics", zap.Duration("interval", config.Statistics.Interval))
		statsExporter.lastEmitCheck = time.Now()
		interval := config.MetricAggregation.Interval.Milliseconds()
		statsExporter.aggregatorI = sampler.NewMetricAggregatorImpl[int64](interval)
		statsExporter.aggregatorF = sampler.NewMetricAggregatorImpl[float64](interval)
		statsExporter.metricTransformations = ottl.Transformations{}
		err := statsExporter.setupMetricTelemetry()
		if err != nil {
			return nil, err
		}
	case "traces":
		statsExporter.spanStats = stats.NewStatsCombiner[*chqpb.SpanStats](now, config.Statistics.Interval)
		statsExporter.logger.Info("sending span statistics", zap.Duration("interval", config.Statistics.Interval))
		statsExporter.traceTransformations = ottl.Transformations{}
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

	return nil
}

func (e *chqEnforcer) Shutdown(ctx context.Context) error {
	var errors *multierror.Error
	e.configExtension.UnregisterCallback(e.configCallbackID)
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
		e.updateLogTransformations(sc)
	case "traces":
		e.updateTraceTransformations(sc)
	case "metrics":
		e.updateMetricSamplingConfig(sc)
	}
	e.logger.Info("Configuration updated")
}

func (e *chqEnforcer) getSlice(l pcommon.Map, key string) pcommon.Slice {
	if field, found := l.Get(key); found {
		return field.Slice()
	}
	return pcommon.NewSlice()
}

func (e *chqEnforcer) sliceContains(l pcommon.Map, key string, value string) bool {
	if field, found := l.Get(key); found {
		slice := field.Slice()
		for i := 0; i < slice.Len(); i++ {
			if slice.At(i).AsString() == value {
				return true
			}
		}
	}
	return false
}

func (e *chqEnforcer) processEnrichments(enrichments []StatsEnrichment, attributesByScope map[string]pcommon.Map) map[string]string {
	tags := make(map[string]string)
	for _, enrichment := range enrichments {
		for scope, attributes := range attributesByScope {
			for _, tag := range enrichment.Tags {
				if tagValue, found := attributes.Get(tag); found {
					key := fmt.Sprintf("%s.%s", scope, tag)
					tags[key] = tagValue.AsString()
				}
			}
		}
	}
	return tags
}

func ToMap(attributes pcommon.Map) map[string]string {
	result := make(map[string]string)
	attributes.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}
