package newrelicreceiver

import (
    "context"
    "time"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/config/confighttp"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/receiver"


	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqnewrelicreceiver/internal/metadata"
	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqnewrelicreceiver/internal/sharedcomponent"
)

func NewFactory() receiver.Factory {
    return receiver.NewFactory(
        metadata.Type,
        createDefaultConfig,
        receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		receiver.WithTraces(createTracesReceiver, metadata.TracesStability),
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
    )
}

func createDefaultConfig() component.Config {
    return &Config{
        ServerConfig: confighttp.ServerConfig{
            Endpoint: "localhost:8126",
        },
        ReadTimeout: 60 * time.Second,
    }
}

func createLogsReceiver(_ context.Context, params receiver.Settings, cfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
    params.Logger.Info("Creating logs receiver")
    rcfg := cfg.(*Config) // Cast component.Config to New Relic-specific Config

    r := receivers.GetOrAdd(cfg, func() component.Component {
        nrReceiver, _ := newNewRelicReceiver(rcfg, params) // Create New Relic receiver instance
        return nrReceiver
    })

    nr := r.Unwrap().(*newRelicReceiver) // Cast the unwrapped component to *newRelicReceiver
    nr.nextLogConsumer = consumer       // Set the log consumer
    nr.logger = params.Logger           // Assign logger for the receiver

    return r, nil // Return the receiver
}

func createTracesReceiver(ctx context.Context, params receiver.Settings, cfg component.Config, consumer consumer.Traces) (receiver.Traces, error) {
    rcfg := cfg.(*Config)
    nrReceiver := &newRelicReceiver{
        config: rcfg,
        logger: params.Logger,
    }
    return nrReceiver, nil // Replace with actual trace receiver implementation
}

func createMetricsReceiver(ctx context.Context, params receiver.Settings, cfg component.Config, consumer consumer.Metrics) (receiver.Metrics, error) {
    rcfg := cfg.(*Config)
    nrReceiver := &newRelicReceiver{
        config: rcfg,
        logger: params.Logger,
    }
    return nrReceiver, nil // Replace with actual metric receiver implementation
}

var receivers = sharedcomponent.NewSharedComponents()