// Copyright 2024 CardinalHQ, Inc
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
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

var xid atomic.Uint64

func (ddr *datadogReceiver) handleLogs(w http.ResponseWriter, req *http.Request) {
	v := xid.Add(1)
	apikey := ddr.showDatadogApiHeaders(req, "V1LOGS", v)
	w.Header().Set("Content-Type", "application/json")

	obsCtx := ddr.tReceiver.StartLogsOp(req.Context())
	var err error
	var logCount int
	defer func(logCount *int) {
		ddr.tReceiver.EndLogsOp(obsCtx, "datadog", *logCount, err)
	}(&logCount)

	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, nil)
		return
	}
	if req.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, nil)
		return
	}

	if apikey == "" {
		ddr.logLogger.Info("V1LOGS No API key found in request", zap.Uint64("XID", v))
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","code":403,"errors":["Forbidden"]`))
		return
	}

	ddLogs, err := handleLogsPayload(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		ddr.logLogger.Error("Unable to unmarshal reqs", zap.Error(err), zap.Uint64("XID", v))
		return
	}
	logCount = len(ddLogs)
	t := pcommon.NewTimestampFromTime(time.Now())
	err = ddr.processLogs(t, ddLogs, v)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		ddr.logLogger.Error("processLogs", zap.Error(err), zap.Uint64("XID", v))
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(""))
}
