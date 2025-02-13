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

package chqmissingdataconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/cardinalhq/cardinalhq-otel-collector/connector/chqmissingdataconnector/internal/metadata"
)

func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithMetricsToMetrics(createMetricsToMetrics, metadata.MetricsToMetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MaximumAge:               defaultMaximumAge,
		Interval:                 defaultInterval,
		NamePrefix:               defaultNamePrefix,
		ResourceAttributesToCopy: defaultResourcesToCopy,
	}
}

func createMetricsToMetrics(_ context.Context, set connector.Settings, cc component.Config, nextConsumer consumer.Metrics) (connector.Metrics, error) {
	cfg := cc.(*Config)
	return &md{
		config:          cfg,
		metricsConsumer: nextConsumer,
		logger:          set.Logger,
		emitterDone:     make(chan struct{}),
	}, nil
}
