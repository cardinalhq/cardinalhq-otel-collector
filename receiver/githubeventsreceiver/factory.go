package githubeventsreceiver

import (
	"context"

	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/githubeventsreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	defaultBindEndpoint = "0.0.0.0:19418"
	defaultPath         = "/events"
	defaultHealthPath   = "/health_check"
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: defaultBindEndpoint,
		},
		Path:       defaultPath,
		HealthPath: defaultHealthPath,
		Secret:     "",
	}

}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	conf := cfg.(*Config)
	return newLogsReceiver(params, conf, consumer)
}
