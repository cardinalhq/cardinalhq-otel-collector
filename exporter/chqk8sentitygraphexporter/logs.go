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

package chqk8sentitygraphexporter

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func (e *exp) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		rattr := rl.Resource().Attributes()
		cid := orgIdFromResource(rattr)
		cache := e.getEntityCache(cid)
		for j := range rl.ScopeLogs().Len() {
			sr := rl.ScopeLogs().At(j)
			for k := range sr.LogRecords().Len() {
				lr := sr.LogRecords().At(k)
				ret, err := e.objecthandler.Feed(rl.Resource().Attributes(), lr.Attributes(), lr.Body())
				if err != nil {
					e.logger.Error("failed to feed log record", zap.Error(err))
					continue
				}
				if ret != nil {
					cache.ProvisionPackagedObject(ret)
				}
			}
		}
	}
	return nil
}
