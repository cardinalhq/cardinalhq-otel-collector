// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadogreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver/internal/metadata"
	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver/internal/sharedcomponent"
)

// NewFactory creates a factory for DataDog receiver.
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

func createTracesReceiver(_ context.Context, params receiver.CreateSettings, cfg component.Config, consumer consumer.Traces) (ret receiver.Traces, err error) {
	params.Logger.Info("Creating traces receiver")
	rcfg := cfg.(*Config)
	r := receivers.GetOrAdd(cfg, func() component.Component {
		dd, _ := newDataDogReceiver(rcfg, params)
		return dd
	})

	ddr := r.Unwrap().(*datadogReceiver)
	ddr.nextTraceConsumer = consumer
	ddr.traceLogger = params.Logger
	return r, nil
}

func createLogsReceiver(_ context.Context, params receiver.CreateSettings, cfg component.Config, consumer consumer.Logs) (ret receiver.Logs, err error) {
	params.Logger.Info("Creating logs receiver")
	rcfg := cfg.(*Config)
	r := receivers.GetOrAdd(cfg, func() component.Component {
		dd, _ := newDataDogReceiver(rcfg, params)
		return dd
	})
	ddr := r.Unwrap().(*datadogReceiver)
	ddr.nextLogConsumer = consumer
	ddr.logLogger = params.Logger
	return r, nil
}

func createMetricsReceiver(_ context.Context, params receiver.CreateSettings, cfg component.Config, consumer consumer.Metrics) (ret receiver.Metrics, err error) {
	params.Logger.Info("Creating metrics receiver")
	rcfg := cfg.(*Config)
	r := receivers.GetOrAdd(cfg, func() component.Component {
		dd, _ := newDataDogReceiver(rcfg, params)
		return dd
	})
	ddr := r.Unwrap().(*datadogReceiver)
	ddr.nextMetricConsumer = consumer
	ddr.metricLogger = params.Logger
	return r, nil
}

var receivers = sharedcomponent.NewSharedComponents()
