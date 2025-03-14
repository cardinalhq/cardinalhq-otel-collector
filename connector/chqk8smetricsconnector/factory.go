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

package chqk8smetricsconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/cardinalhq/cardinalhq-otel-collector/connector/chqk8smetricsconnector/internal/metadata"
)

// NewFactory creates a new factory for the CardinalHQ Kubernetes Metrics Connector.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithLogsToMetrics(createLogsToMetrics, metadata.LogsToMetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return getDefaultConfig()
}

func getDefaultConfig() *Config {
	return &Config{
		Events: EventsConfig{
			Interval: defaultEventsReportingInterval,
		},
	}
}

func createLogsToMetrics(_ context.Context, set connector.Settings, cc component.Config, nextConsumer consumer.Metrics) (connector.Logs, error) {
	cfg := cc.(*Config)
	return &md{
		id:              set.ID,
		config:          cfg,
		metricsConsumer: nextConsumer,
		logger:          set.Logger,
		emitterDone:     make(chan struct{}),
		converters:      buildConverters(),
	}, nil
}
