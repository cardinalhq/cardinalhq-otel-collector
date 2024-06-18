// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadogreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver"

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	ddpbtrace "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
	"github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type datadogReceiver struct {
	address             string
	config              *Config
	nextTraceConsumer   consumer.Traces
	nextLogConsumer     consumer.Logs
	nextMetricConsumer  consumer.Metrics
	traceLogger         *zap.Logger
	logLogger           *zap.Logger
	metricLogger        *zap.Logger
	gpLogger            *zap.Logger
	telemetrySettings   component.TelemetrySettings
	server              *http.Server
	obsrecv             *receiverhelper.ObsReport
	metricFilterCounter metric.Int64Counter
	podName             string
	id                  string
}

func newDataDogReceiver(config *Config, params receiver.Settings) (component.Component, error) {
	instance, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{LongLivedCtx: false, ReceiverID: params.ID, Transport: "http", ReceiverCreateSettings: params})
	if err != nil {
		return nil, err
	}

	ddr := &datadogReceiver{
		telemetrySettings: params.TelemetrySettings,
		config:            config,
		server: &http.Server{
			ReadTimeout: config.ReadTimeout,
		},
		obsrecv: instance,
		id:      params.ID.String(),
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "unknown"
	}
	ddr.podName = podName

	if ddr.gpLogger == nil {
		ddr.gpLogger = params.Logger.With(zap.String("data_type", "internal"))
	}

	m, err := metadata.Meter(ddr.telemetrySettings).Int64Counter(params.ID.Type().String()+".receiver.datapoints.agechecked",
		metric.WithDescription("The number of metrics filtered out by the Datadog receiver"))
	if err != nil {
		return nil, err
	}
	ddr.metricFilterCounter = m

	return ddr, nil
}

func (ddr *datadogReceiver) Start(ctx context.Context, host component.Host) error {
	ddmux := http.NewServeMux()

	if ddr.nextTraceConsumer != nil {
		ddr.traceLogger.Info("datadog receiver listening for traces")
		ddmux.HandleFunc("/v0.3/traces", ddr.handleTraces)
		ddmux.HandleFunc("/v0.4/traces", ddr.handleTraces)
		ddmux.HandleFunc("/v0.5/traces", ddr.handleTraces)
		ddmux.HandleFunc("/v0.7/traces", ddr.handleTraces)
		ddmux.HandleFunc("/api/v0.2/traces", ddr.handleTraces)
	}

	if ddr.nextLogConsumer != nil {
		ddr.logLogger.Info("datadog receiver listening for logs")
		ddmux.HandleFunc("/api/v2/logs", ddr.handleLogs)
	}

	if ddr.nextMetricConsumer != nil {
		ddr.metricLogger.Info("datadog receiver listening for metrics")
		ddmux.HandleFunc("/api/v1/series", ddr.handleV1Series)
		ddmux.HandleFunc("/api/v2/series", ddr.handleV2Series)
	}

	ddmux.HandleFunc("/api/v1/validate", ddr.handleV1Validate)
	ddmux.HandleFunc("/intake", ddr.handleIntake)
	ddmux.HandleFunc("/intake/", ddr.handleIntake)
	ddmux.HandleFunc("/api/v1/check_run", ddr.handleCheckRun)
	ddmux.HandleFunc("/api/v1/metadata", ddr.handleMetadata)

	var err error
	ddr.server, err = ddr.config.ServerConfig.ToServer(
		ctx,
		host,
		ddr.telemetrySettings,
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
			ddr.telemetrySettings.ReportStatus(component.NewFatalErrorEvent(fmt.Errorf("error starting datadog receiver: %w", err)))
		}
	}()
	return nil
}

func (ddr *datadogReceiver) Shutdown(ctx context.Context) (err error) {
	return ddr.server.Shutdown(ctx)
}

func getDDAPIKey(req *http.Request) string {
	if apikey := req.Header.Get("DD-API-KEY"); apikey != "" {
		req.Header.Del("DD-API-KEY")
		return apikey
	}
	if apikey := req.URL.Query().Get("DD-API-KEY"); apikey != "" {
		q := req.URL.Query()
		q.Del("DD-API-KEY")
		req.URL.RawQuery = q.Encode()
		return apikey
	}
	if apikey := req.URL.Query().Get("api_key"); apikey != "" {
		q := req.URL.Query()
		q.Del("api_key")
		req.URL.RawQuery = q.Encode()
		return apikey
	}
	return ""
}

func hexdump(data []byte) {
	var line string
	for _, b := range data {
		line += fmt.Sprintf("%02x", b)
	}
	fmt.Println(line)
}

func (ddr *datadogReceiver) handleV1Validate(w http.ResponseWriter, req *http.Request) {
	apikey := getDDAPIKey(req)
	w.Header().Set("Content-Type", "application/json")
	if apikey == "" {
		ddr.gpLogger.Info("/api/v1/validate No API key found in request")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","code":403,"errors":["Forbidden"]`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"valid":"true"}`))
}

func (ddr *datadogReceiver) handleIntake(w http.ResponseWriter, req *http.Request) {
	apikey := getDDAPIKey(req)
	w.Header().Set("Content-Type", "application/json")
	if apikey == "" {
		ddr.gpLogger.Info("/intake No API key found in request")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","code":403,"errors":["Forbidden"]`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (ddr *datadogReceiver) handleCheckRun(w http.ResponseWriter, req *http.Request) {
	apikey := getDDAPIKey(req)
	w.Header().Set("Content-Type", "application/json")
	if apikey == "" {
		ddr.gpLogger.Info("/api/v1/check_run No API key found in request")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","code":403,"errors":["Forbidden"]`))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (ddr *datadogReceiver) handleMetadata(w http.ResponseWriter, req *http.Request) {
	apikey := getDDAPIKey(req)
	w.Header().Set("Content-Type", "application/json")
	if apikey == "" {
		ddr.gpLogger.Info("/api/v1/metadata No API key found in request")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","code":403,"errors":["Forbidden"]`))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (ddr *datadogReceiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	apikey := getDDAPIKey(req)
	w.Header().Set("Content-Type", "application/json")

	obsCtx := ddr.obsrecv.StartTracesOp(req.Context())
	var err error
	var spanCount int
	defer func(spanCount *int) {
		ddr.obsrecv.EndTracesOp(obsCtx, "datadog", *spanCount, err)
	}(&spanCount)

	var ddTraces []*ddpbtrace.TracerPayload

	if apikey == "" {
		ddr.traceLogger.Info("TRACES No API key found in request")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","code":403,"errors":["Forbidden"]`))
		return
	}

	ddTraces, err = handlePayload(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		ddr.traceLogger.Error("Unable to unmarshal reqs")
		return
	}
	for _, ddTrace := range ddTraces {
		otelTraces := toTraces(ddTrace, req)
		spanCount = otelTraces.SpanCount()
		err = ddr.nextTraceConsumer.ConsumeTraces(obsCtx, otelTraces)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			ddr.traceLogger.Error("Trace consumer errored out")
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
