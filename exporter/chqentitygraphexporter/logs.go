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
	"context"
	"github.com/cardinalhq/oteltools/pkg/graph"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func (e *entityGraphExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	failingPods := make([]*graph.K8SPodObject, 0)

	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		resourceAttributes := rl.Resource().Attributes()
		cid := OrgIdFromResource(resourceAttributes)
		cache := e.GetEntityCache(cid)
		globalEntityMap := cache.ProvisionResourceAttributes(resourceAttributes)

		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)
			for k := range sl.LogRecords().Len() {
				lr := sl.LogRecords().At(k)
				podObject := graph.ExtractPodObject(lr)
				if podObject != nil {
					if podObject.IsImagePullBackOff || podObject.IsCrashLoopBackOff || podObject.IsOOMKilled {
						failingPods = append(failingPods, podObject)
					}
				}
				cache.ProvisionRecordAttributes(globalEntityMap, lr.Attributes())
			}
		}
	}

	// Tell external-api about backing off pods, so it can process them as events.
	if len(failingPods) > 0 {
		go func() {
			err := e.postBackOffEvents(context.Background(), failingPods)
			if err != nil {
				e.logger.Error("Failed to send backoff event", zap.Error(err))
			}
		}()
	}
	return nil
}
