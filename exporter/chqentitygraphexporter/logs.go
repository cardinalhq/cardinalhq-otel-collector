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
	"go.opentelemetry.io/collector/pdata/plog"
)

func (e *entityGraphExporter) ConsumeLogs(ctx context.Context, td plog.Logs) error {
	for i := range td.ResourceLogs().Len() {
		rs := td.ResourceLogs().At(i)
		resourceAttributes := rs.Resource().Attributes()
		cid := orgIdFromResource(resourceAttributes)
		cache := e.getEntityCache(cid)
		globalEntityMap := cache.ProvisionResourceAttributes(resourceAttributes)

		for j := range rs.ScopeLogs().Len() {
			iss := rs.ScopeLogs().At(j)
			for k := range iss.LogRecords().Len() {
				sr := iss.LogRecords().At(k)
				attributes := sr.Attributes()
				cache.ProvisionRecordAttributes(globalEntityMap, attributes)
			}
		}
	}
	return nil
}
