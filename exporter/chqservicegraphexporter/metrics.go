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
	requestsMetric  = "traces_service_graph_request_total"
	errorsMetric    = "traces_service_graph_request_failed_total"
	client          = "client"
	clientCluster   = "client__cardinalhq.k8s.cluster.name"
	clientNamespace = "client__cardinalhq.k8s.namespace.name"
	serverCluster   = "server__cardinalhq.k8s.cluster.name"
	serverNamespace = "server__cardinalhq.k8s.namespace.name"
	server          = "server"
	connectionType  = "connection_type"
)

func (e *serviceGraphExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	cachesTouched := map[string]*EdgeCache{}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		orgID := OrgIdFromResource(rm.Resource().Attributes())
		edgeCache := e.getEdgeCache(orgID)
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
							clientCluster, clientClusterFound := attributes.Get(clientCluster)
							clientNamespace, clientNamespaceFound := attributes.Get(clientNamespace)
							serverCluster, serverClusterFound := attributes.Get(serverCluster)
							serverNamespace, serverNamespaceFound := attributes.Get(serverNamespace)

							shouldConnect := !connectionTypeFound || cType.AsString() != "messaging_system" && cType.AsString() != "database"
							if clientFound && serverFound && shouldConnect && clientClusterFound && clientNamespaceFound && serverClusterFound && serverNamespaceFound {
								edge := Edge{
									ClientClusterName: clientCluster.AsString(),
									ClientNamespace:   clientNamespace.AsString(),
									ServerClusterName: serverCluster.AsString(),
									ServerNamespace:   serverNamespace.AsString(),
									Client:            clientService.AsString(),
									Server:            serverService.AsString(),
									ConnectionType:    cType.AsString(),
								}
								edgeCache.Add(edge)
								cachesTouched[orgID] = edgeCache
							}
						}
					}
				}
			}
		}
	}

	for cid, cache := range cachesTouched {
		edges := cache.flush()
		if len(edges) > 0 {
			go func() {
				timeout := e.config.Timeout
				if timeout == 0 {
					timeout = 5 * time.Second
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				err := e.postEdges(ctx, cid, edges)
				if err != nil {
					e.logger.Error("Failed to send edges", zap.Error(err))
				}
			}()
		}
	}
	return nil
}

func (e *serviceGraphExporter) postEdges(ctx context.Context, cid string, edges []Edge) error {
	endpoint := e.config.Endpoint + "/api/v1/edges?organizationID=" + cid
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
		e.logger.Error("Failed to send edges", zap.Int("status", resp.StatusCode), zap.String("body", string(body)), zap.String("endpoint", endpoint))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
