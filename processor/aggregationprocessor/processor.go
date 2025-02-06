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

package aggregationprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/aggregationprocessor/internal/metadata"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
)

type MetricsConsumer interface {
	ConsumeMetrics(ctx context.Context, td pmetric.Metrics) error
}

type aggregationProcessor struct {
	sync.RWMutex

	config *Config
	logger *zap.Logger

	id                component.ID
	ttype             string
	telemetrySettings component.TelemetrySettings

	additionalAttributes map[string]string

	// for metrics
	nextMetricReceiver   MetricsConsumer
	aggregationInterval  time.Duration
	aggregatorI          ottl.MetricAggregator[int64]
	aggregatorF          ottl.MetricAggregator[float64]
	lastEmitCheck        time.Time
	aggregatedDatapoints *telemetry.DeferrableInt64Counter
}

func newPitbull(config *Config, ttype string, set processor.Settings, nextConsumer consumer.Metrics) (*aggregationProcessor, error) {
	p := &aggregationProcessor{
		id:                   set.ID,
		ttype:                ttype,
		config:               config,
		telemetrySettings:    set.TelemetrySettings,
		additionalAttributes: config.AdditionalAttributes,
		logger:               set.Logger,
		nextMetricReceiver:   nextConsumer,
	}

	attrset := attribute.NewSet(
		attribute.String("processor", set.ID.String()),
		attribute.String("signal", ttype),
	)

	p.lastEmitCheck = time.Now()
	interval := config.MetricAggregation.Interval.Milliseconds()
	p.aggregatorI = ottl.NewMetricAggregatorImpl[int64](interval)
	p.aggregatorF = ottl.NewMetricAggregatorImpl[float64](interval)
	err := p.setupMetricTelemetry(set, attrset)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *aggregationProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *aggregationProcessor) setupMetricTelemetry(set processor.Settings, attrset attribute.Set) error {
	counter, err := telemetry.NewDeferrableInt64Counter(metadata.Meter(set.TelemetrySettings),
		"aggregation_datapoints_processed",
		[]metric.Int64CounterOption{
			metric.WithDescription("The number of datapoints processed by the aggregation processor"),
			metric.WithUnit("1"),
		},
		[]metric.AddOption{
			metric.WithAttributeSet(attrset),
		},
	)
	if err != nil {
		return err
	}
	p.aggregatedDatapoints = counter
	return nil
}
