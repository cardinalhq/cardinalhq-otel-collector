package chqmissingdataconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/cardinalhq/cardinalhq-otel-collector/connector/chqmissingdataconnector/internal/metadata"
)

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithMetricsToMetrics(createMetricsToMetrics, metadata.MetricsToMetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

// createMetricsToMetrics creates a metricds to metrics connector based on provided config.
func createMetricsToMetrics(_ context.Context, set connector.Settings, _ component.Config, nextConsumer consumer.Metrics) (connector.Metrics, error) {
	return &md{
		metricsConsumer: nextConsumer,
		logger:          set.Logger,
	}, nil
}
