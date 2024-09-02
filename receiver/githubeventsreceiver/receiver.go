package githubeventsreceiver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/google/go-github/v63/github"
	"github.com/julienschmidt/httprouter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

var (
	errNilLogsConsumer = errors.New("missing a logs consumer")
	errMissingEndpoint = errors.New("missing a receiver endpoint")
)

type githubEventsReceiver struct {
	settings    receiver.Settings
	cfg         *Config
	logConsumer consumer.Logs
	server      *http.Server
	shutdownWG  sync.WaitGroup
	obsrecv     *receiverhelper.ObsReport
	logger      *zap.Logger
}

func newLogsReceiver(params receiver.Settings, cfg *Config, consumer consumer.Logs) (receiver.Logs, error) {
	if consumer == nil {
		return nil, errNilLogsConsumer
	}

	if cfg.Endpoint == "" {
		return nil, errMissingEndpoint
	}

	transport := "http"
	if cfg.TLSSetting != nil {
		transport = "https"
	}

	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             params.ID,
		Transport:              transport,
		ReceiverCreateSettings: params,
	})

	if err != nil {
		return nil, err
	}

	return &githubEventsReceiver{
		settings:    params,
		cfg:         cfg,
		logConsumer: consumer,
		logger:      params.Logger,
		obsrecv:     obsrecv,
	}, nil
}

func (g *githubEventsReceiver) Start(ctx context.Context, host component.Host) error {
	router := httprouter.New()

	router.HandlerFunc(http.MethodPost, g.cfg.Path, g.handleGHEvents)
	router.HandlerFunc(http.MethodGet, g.cfg.HealthPath, g.handleHealthCheck)

	var err error
	g.server, err = g.cfg.ServerConfig.ToServer(ctx, host, g.settings.TelemetrySettings, router)
	if err != nil {
		return fmt.Errorf("failed to create server definition: %w", err)
	}

	listener, err := g.cfg.ServerConfig.ToListener(ctx)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	g.shutdownWG.Add(1)
	go func() {
		defer g.shutdownWG.Done()
		if err := g.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
		}

	}()

	return nil

}

func (g *githubEventsReceiver) Shutdown(ctx context.Context) error {
	// server must exist to be closed.
	if g.server == nil {
		return nil
	}

	err := g.server.Close()
	g.shutdownWG.Wait()
	return err

}

func (g *githubEventsReceiver) handleGHEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Validate the incoming webhook and return the json payload
	payload, err := github.ValidatePayload(r, []byte(g.cfg.Secret))
	if err != nil {
		g.logger.Error("Payload validation failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	eventType := github.WebHookType(r)
	event, err := github.ParseWebHook(eventType, payload)
	if err != nil {
		g.logger.Error("Webhook parsing failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch event := event.(type) {
	case *github.WorkflowJobEvent, *github.WorkflowRunEvent:
		logs := createLogs(event, payload)
		if err := g.logConsumer.ConsumeLogs(ctx, logs); err != nil {
			g.logger.Error("Error occurred in ConsumeLogs", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	default:
		g.logger.Debug("Skipping unsupported event type", zap.String("event", eventType))
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.WriteHeader(http.StatusAccepted)

}

func (g *githubEventsReceiver) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte("OK"))

}
