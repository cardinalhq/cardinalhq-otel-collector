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

package datadogreceiver

import (
	"net/http"

	"go.uber.org/zap"
)

func (ddr *datadogReceiver) handleLogs(w http.ResponseWriter, req *http.Request) {
	if ddr.nextLogConsumer == nil {
		http.Error(w, "Consumer not initialized", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	ctx := ddr.obsrecv.StartLogsOp(req.Context())
	var err error
	var logCount int
	defer func(logCount *int) {
		ddr.obsrecv.EndLogsOp(ctx, "datadog", *logCount, err)
	}(&logCount)

	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, nil)
		return
	}
	if req.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, nil)
		return
	}

	ddLogs, err := handleLogsPayload(req, ddr.logLogger)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		ddr.logLogger.Warn("Unable to unmarshal reqs", zap.Error(err), zap.Any("httpHeaders", req.Header))
		return
	}
	logCount = len(ddLogs)
	apikey := getDDAPIKey(req)
	err = ddr.processLogs(ctx, apikey, ddLogs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		ddr.logLogger.Error("processLogs", zap.Error(err))
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(""))
}
