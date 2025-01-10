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
	"context"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func (ddr *datadogReceiver) handleV1Series(w http.ResponseWriter, req *http.Request) {
	if ddr.nextMetricConsumer == nil {
		http.Error(w, "Consumer not initialized", http.StatusServiceUnavailable)
		return
	}
	if req.Method != http.MethodPost && req.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, nil)
		return
	}

	cc, done := context.WithTimeout(req.Context(), 1*time.Minute)
	defer done()
	ctx := ddr.obsrecv.StartMetricsOp(cc)
	var err error
	var metricCount int
	defer func(metricCount *int) {
		ddr.obsrecv.EndMetricsOp(ctx, "datadog", *metricCount, err)
	}(&metricCount)

	ddMetrics, httpCode, err := handleMetricsV1Payload(req)
	if err != nil {
		writeError(w, httpCode, err)
		ddr.metricLogger.Warn("Unable to unmarshal reqs", zap.Error(err), zap.Any("httpHeaders", req.Header))
		return
	}
	metricCount = len(ddMetrics)
	apikey := getDDAPIKey(req)
	err = ddr.processMetricsV1(ctx, apikey, ddMetrics)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		ddr.metricLogger.Error("processMetrics", zap.Error(err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (ddr *datadogReceiver) handleV2Series(w http.ResponseWriter, req *http.Request) {
	if ddr.nextMetricConsumer == nil {
		http.Error(w, "Consumer not initialized", http.StatusServiceUnavailable)
		return
	}
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, nil)
		return
	}

	ctx := ddr.obsrecv.StartMetricsOp(req.Context())
	var err error
	var metricCount int
	defer func(metricCount *int) {
		ddr.obsrecv.EndMetricsOp(ctx, "datadog", *metricCount, err)
	}(&metricCount)

	ddMetrics, httpCode, err := ddr.handleMetricsV2Payload(req)
	if err != nil {
		ddr.metricLogger.Warn("Unable to unmarshal reqs", zap.Error(err), zap.Any("httpHeaders", req.Header))
		writeError(w, httpCode, err)
		return
	}
	metricCount = len(ddMetrics)
	err = ddr.processMetricsV2(ctx, getDDAPIKey(req), ddMetrics)
	if err != nil {
		ddr.metricLogger.Error("processMetrics", zap.Error(err))
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
