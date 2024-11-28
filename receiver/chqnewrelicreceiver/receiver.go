package newrelicreceiver

import (
    "context"
    "encoding/json"
    "net/http"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/receiver"
    "go.uber.org/zap"
)

type newRelicReceiver struct {
    config          *Config
    nextLogConsumer consumer.Logs
    logger          *zap.Logger
    server          *http.Server
}

func (nr *newRelicReceiver) Start(ctx context.Context, host component.Host) error {
    nr.logger.Info("Starting New Relic receiver")
    mux := http.NewServeMux()
    mux.HandleFunc("/logs", nr.handleLogs)
    nr.server = &http.Server{
        Addr:    nr.config.Endpoint,
        Handler: mux,
    }
    go func() {
        if err := nr.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            nr.logger.Error("Failed to start server", zap.Error(err))
        }
    }()
    return nil
}

func (nr *newRelicReceiver) handleLogs(w http.ResponseWriter, req *http.Request) {
    if nr.nextLogConsumer == nil {
        http.Error(w, "Consumer not initialized", http.StatusServiceUnavailable)
        return
    }

    if req.Header.Get("X-Insert-Key") != nr.config.APIKey {
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return
    }

    var logs []newRelicLog
    err := json.NewDecoder(req.Body).Decode(&logs)
    if err != nil {
        http.Error(w, "Invalid payload", http.StatusBadRequest)
        return
    }

    otelLogs := convertNewRelicLogsToOTEL(logs)
    err = nr.nextLogConsumer.ConsumeLogs(req.Context(), otelLogs)
    if err != nil {
        http.Error(w, "Failed to process logs", http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusAccepted)
}

func (nr *newRelicReceiver) Shutdown(ctx context.Context) error {
    nr.logger.Info("Shutting down New Relic receiver")
    if nr.server != nil {
        return nr.server.Shutdown(ctx)
    }
    return nil
}

func newNewRelicReceiver(cfg *Config, settings receiver.Settings) (*newRelicReceiver, error) {
    return &newRelicReceiver{
        config: cfg,
        logger: settings.Logger,
    }, nil
}

// Exported function to retrieve the API key from a request
func GetNewRelicAPIKey(req *http.Request) string {
    if apiKey := req.Header.Get("X-Insert-Key"); apiKey != "" {
        return apiKey
    }
    return ""
}
