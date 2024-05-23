// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadogreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver"

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	ddpbtrace "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type datadogReceiver struct {
	address            string
	config             *Config
	params             receiver.CreateSettings
	nextTraceConsumer  consumer.Traces
	nextLogConsumer    consumer.Logs
	nextMetricConsumer consumer.Metrics
	server             *http.Server
	tReceiver          *receiverhelper.ObsReport
}

func newDataDogReceiver(config *Config, params receiver.CreateSettings) (component.Component, error) {
	instance, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{LongLivedCtx: false, ReceiverID: params.ID, Transport: "http", ReceiverCreateSettings: params})
	if err != nil {
		return nil, err
	}

	return &datadogReceiver{
		params: params,
		config: config,
		server: &http.Server{
			ReadTimeout: config.ReadTimeout,
		},
		tReceiver: instance,
	}, nil
}

func (ddr *datadogReceiver) Start(ctx context.Context, host component.Host) error {
	ddmux := http.NewServeMux()

	if ddr.nextTraceConsumer != nil {
		ddr.params.Logger.Info("datadog receiver listening for traces")
		ddmux.HandleFunc("/v0.3/traces", ddr.handleTraces)
		ddmux.HandleFunc("/v0.4/traces", ddr.handleTraces)
		ddmux.HandleFunc("/v0.5/traces", ddr.handleTraces)
		ddmux.HandleFunc("/v0.7/traces", ddr.handleTraces)
		ddmux.HandleFunc("/api/v0.2/traces", ddr.handleTraces)
	}

	if ddr.nextLogConsumer != nil {
		ddr.params.Logger.Info("datadog receiver listening for logs")
		ddmux.HandleFunc("/api/v2/logs", ddr.handleLogs)
	}

	if ddr.nextMetricConsumer != nil {
		ddr.params.Logger.Info("datadog receiver listening for metrics")
		ddmux.HandleFunc("/api/v1/series", ddr.handleV1Series)
		ddmux.HandleFunc("/api/v2/series", ddr.handleV2Series)
	}

	ddmux.HandleFunc("/api/v1/validate", ddr.handleV1Validate)
	ddmux.HandleFunc("/intake", ddr.handleIntake)

	var err error
	ddr.server, err = ddr.config.ServerConfig.ToServer(
		ctx,
		host,
		ddr.params.TelemetrySettings,
		ddmux,
	)
	if err != nil {
		return fmt.Errorf("failed to create server definition: %w", err)
	}
	hln, err := ddr.config.ServerConfig.ToListener(ctx)
	if err != nil {
		return fmt.Errorf("failed to create datadog listener: %w", err)
	}

	ddr.address = hln.Addr().String()

	go func() {
		if err := ddr.server.Serve(hln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			ddr.params.TelemetrySettings.ReportStatus(component.NewFatalErrorEvent(fmt.Errorf("error starting datadog receiver: %w", err)))
		}
	}()
	return nil
}

func (ddr *datadogReceiver) Shutdown(ctx context.Context) (err error) {
	return ddr.server.Shutdown(ctx)
}

func (ddr *datadogReceiver) showBodyIfJson(req *http.Request, source string) {
	if req.Header.Get("Content-Type") == "application/json" {
		buffer := make([]byte, req.ContentLength)
		_, _ = req.Body.Read(buffer)
		ddr.params.Logger.Info("message body", zap.String("endpoint", source), zap.String("json", string(buffer)))
	}
}

func (ddr *datadogReceiver) showDatadogApiHeaders(req *http.Request, source string) {
	apikey := req.Header.Get("DD-API-KEY")
	if apikey != "" {
		keylen := len(apikey)
		if keylen > 4 {
			apikey = apikey[:4] + "..."
		}
	}
	ddr.params.Logger.Info("datadog api headers", zap.String("source", source), zap.String("DD-API-KEY", apikey), zap.String("content-type", req.Header.Get("Content-Type")), zap.String("content-encoding", req.Header.Get("Content-Encoding")))
}

func (ddr *datadogReceiver) handleV1Validate(w http.ResponseWriter, req *http.Request) {
	ddr.showDatadogApiHeaders(req, "/api/v1/validate")
	ddr.showBodyIfJson(req, "/api/v1/validate")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"valid":"ok"}`))
}

func (ddr *datadogReceiver) handleIntake(w http.ResponseWriter, req *http.Request) {
	ddr.showDatadogApiHeaders(req, "/intake")
	ddr.showBodyIfJson(req, "/intake")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (ddr *datadogReceiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	obsCtx := ddr.tReceiver.StartTracesOp(req.Context())
	var err error
	var spanCount int
	defer func(spanCount *int) {
		ddr.tReceiver.EndTracesOp(obsCtx, "datadog", *spanCount, err)
	}(&spanCount)

	var ddTraces []*ddpbtrace.TracerPayload
	ddTraces, err = handlePayload(req)
	if err != nil {
		http.Error(w, "Unable to unmarshal reqs", http.StatusBadRequest)
		ddr.params.Logger.Error("Unable to unmarshal reqs")
		return
	}
	for _, ddTrace := range ddTraces {
		otelTraces := toTraces(ddTrace, req)
		spanCount = otelTraces.SpanCount()
		err = ddr.nextTraceConsumer.ConsumeTraces(obsCtx, otelTraces)
		if err != nil {
			http.Error(w, "Trace consumer errored out", http.StatusInternalServerError)
			ddr.params.Logger.Error("Trace consumer errored out")
			return
		}
	}

	_, _ = w.Write([]byte("OK"))
}
