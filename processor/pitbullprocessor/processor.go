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
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor/internal/metadata"
)

type pitbull struct {
	sync.RWMutex

	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings
	configExtension   *chqconfigextension.CHQConfigExtension
	configCallbackID  int
	podName           string

	// for logs
	logTransformations ottl.Transformations
	logFingerprinter   fingerprinter.Fingerprinter

	// for spans
	traceTransformations ottl.Transformations
	traceFingerprinter   fingerprinter.Fingerprinter
	estimators           map[uint64]*SlidingEstimatorStat
	estimatorWindowSize  int
	estimatorInterval    int64

	// for metrics
	metricTransformations ottl.Transformations

	nextMetricReceiver   consumer.Metrics
	aggregationInterval  time.Duration
	aggregatorI          ottl.MetricAggregator[int64]
	aggregatorF          ottl.MetricAggregator[float64]
	lastEmitCheck        time.Time
	aggregatedDatapoints *ottl.TransformCounter

	ottlProcessed *ottl.TransformCounter
}

func newPitbull(config *Config, ttype string, set processor.Settings, nextConsumer consumer.Metrics) (*pitbull, error) {
	dog := &pitbull{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		nextMetricReceiver: nextConsumer,
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
	dog.ottlProcessed = counter

	switch ttype {
	case "logs":
		dog.logTransformations = ottl.Transformations{}
		dog.logFingerprinter = fingerprinter.NewFingerprinter()

	case "metrics":
		dog.lastEmitCheck = time.Now()
		interval := config.MetricAggregation.Interval.Milliseconds()
		dog.aggregatorI = ottl.NewMetricAggregatorImpl[int64](interval)
		dog.aggregatorF = ottl.NewMetricAggregatorImpl[float64](interval)
		dog.metricTransformations = ottl.Transformations{}
		err := dog.setupMetricTelemetry(set, attrset)
		if err != nil {
			return nil, err
		}
	case "traces":
		dog.traceTransformations = ottl.Transformations{}
		dog.traceFingerprinter = fingerprinter.NewFingerprinter()
		dog.estimators = make(map[uint64]*SlidingEstimatorStat)
		dog.estimatorWindowSize = config.TracesConfig.EstimatorWindowSize
		dog.estimatorInterval = config.TracesConfig.EstimatorInterval
	}

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
	var errors *multierror.Error
	e.configExtension.UnregisterCallback(e.configCallbackID)
	return errors.ErrorOrNil()
}

func (e *pitbull) setupMetricTelemetry(set processor.Settings, attrset attribute.Set) error {
	counter, err := ottl.NewTransformCounter(metadata.Meter(set.TelemetrySettings),
		"cardinalhq.processor.pitbull.ottl_processed",
		[]metric.Int64CounterOption{
			metric.WithDescription("The results of OTTL processing"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if err != nil {
		return err
	}
	e.aggregatedDatapoints = counter
	return nil
}

func (e *pitbull) configUpdateCallback(sc ottl.SamplerConfig) {
	switch e.ttype {
	case "logs":
		e.updateLogTransformations(sc)
	case "traces":
		e.updateTraceTransformations(sc)
	case "metrics":
		e.updateMetricTransformation(sc)
	}
	e.logger.Info("Configuration updated")
}

func (e *pitbull) processEnrichments(enrichments []StatsEnrichment, attributesByScope map[string]pcommon.Map) map[string]string {
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
