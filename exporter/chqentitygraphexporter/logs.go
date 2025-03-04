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

package chqentitygraphexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/graph"
	"go.uber.org/zap"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/pdata/plog"
)

func (e *entityGraphExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	failingPods := make([]*graph.K8SPodObject, 0)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttributes := rl.Resource().Attributes()
		cid := OrgIdFromResource(resourceAttributes)
		cache := e.GetEntityCache(cid)
		globalEntityMap := cache.ProvisionResourceAttributes(resourceAttributes)

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				podObject := graph.ExtractPodObject(lr)
				if podObject != nil {
					cache.ProvisionPod(podObject)
					if podObject.IsImagePullBackOff || podObject.IsCrashLoopBackOff || podObject.IsOOMKilled {
						failingPods = append(failingPods, podObject)
					}
				}
				//deployment := graph.ExtractDeploymentObject(lr)
				//if deployment != nil {
				//	cache.ProvisionDeployment(deployment)
				//	isK8sObject = true
				//}
				//statefulSet := graph.ExtractStatefulSetObject(lr)
				//if statefulSet != nil {
				//	cache.ProvisionStatefulSet(statefulSet)
				//	isK8sObject = true
				//}
				cache.ProvisionRecordAttributes(globalEntityMap, lr.Attributes())
			}
		}
	}

	// Tell external-api about backing off pods, so it can process them as events.
	if len(failingPods) > 0 {
		go func() {
			err := e.postBackOffEvent(ctx, failingPods)
			if err != nil {
				e.logger.Error("Failed to send backoff event", zap.Error(err))
			}
		}()
	}
	return nil
}

func (e *entityGraphExporter) postBackOffEvent(ctx context.Context, events []*graph.K8SPodObject) error {
	endpoint := e.config.Endpoint + "/api/v1/backOffEvents"
	b, err := json.Marshal(events)
	e.logger.Info("Sending backoff event", zap.String("endpoint", endpoint), zap.String("events", string(b)))
	if err != nil {
		return fmt.Errorf("failed to marshal kube events: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			e.logger.Warn("Request canceled", zap.Error(ctx.Err()))
			return nil
		}
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send kube events",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
