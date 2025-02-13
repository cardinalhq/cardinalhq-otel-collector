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
		MaximumAge: defaultMaximumAge,
		Interval:   defaultInterval,
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
