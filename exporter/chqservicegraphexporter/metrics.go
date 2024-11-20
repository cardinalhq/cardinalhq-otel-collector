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

package chqservicegraphexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	requestsMetric = "traces_service_graph_request_total"
	errorsMetric   = "traces_service_graph_request_failed_total"
	client         = "client"
	server         = "server"
	connectionType = "connection_type"
)

func (e *serviceGraphExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				metric := ilm.Metrics().At(k)
				name := metric.Name()

				if metric.Type() == pmetric.MetricTypeSum && (name == requestsMetric || name == errorsMetric) {
					if metric.Sum().DataPoints().Len() > 0 {
						for l := 0; l < metric.Sum().DataPoints().Len(); l++ {
							dp := metric.Sum().DataPoints().At(l)
							attributes := dp.Attributes()
							clientService, clientFound := attributes.Get(client)
							serverService, serverFound := attributes.Get(server)
							cType, connectionTypeFound := attributes.Get(connectionType)
							if clientFound && serverFound && connectionTypeFound {
								edge := Edge{
									Client:         clientService.AsString(),
									Server:         serverService.AsString(),
									ConnectionType: cType.AsString(),
								}
								e.edgeCache.Add(edge)
							}
						}
					}
				}
			}
		}
	}
	edges := e.edgeCache.flush()
	if len(edges) > 0 {
		go func() {
			timeout := e.config.Timeout
			if timeout == 0 {
				timeout = 5 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			err := e.postEdges(ctx, edges)
			if err != nil {
				e.logger.Error("Failed to send edges", zap.Error(err))
			}
		}()
	}
	return nil
}

func (e *serviceGraphExporter) postEdges(ctx context.Context, edges []Edge) error {
	endpoint := e.config.Endpoint + "/api/v1/edges"
	b, err := json.Marshal(edges)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send edges", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
