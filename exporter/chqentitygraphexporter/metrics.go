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

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (e *entityGraphExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		rattr := rm.Resource().Attributes()
		cid := OrgIdFromResource(rattr)
		cache := e.GetEntityCache(cid)

		globalEntityMap := cache.ProvisionResourceAttributes(rattr)

		for j := range rm.ScopeMetrics().Len() {
			ilm := rm.ScopeMetrics().At(j)
			for k := range ilm.Metrics().Len() {
				m := ilm.Metrics().At(k)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := range m.Gauge().DataPoints().Len() {
						dp := m.Gauge().DataPoints().At(l)
						cache.ProvisionRecordAttributes(globalEntityMap, dp.Attributes())
					}
				case pmetric.MetricTypeSum:
					for l := range m.Sum().DataPoints().Len() {
						dp := m.Sum().DataPoints().At(l)
						cache.ProvisionRecordAttributes(globalEntityMap, dp.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for l := range m.Histogram().DataPoints().Len() {
						dp := m.Histogram().DataPoints().At(l)
						cache.ProvisionRecordAttributes(globalEntityMap, dp.Attributes())
					}
				case pmetric.MetricTypeSummary:
					for l := range m.Summary().DataPoints().Len() {
						dp := m.Summary().DataPoints().At(l)
						cache.ProvisionRecordAttributes(globalEntityMap, dp.Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := range m.ExponentialHistogram().DataPoints().Len() {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						cache.ProvisionRecordAttributes(globalEntityMap, dp.Attributes())
					}
				}
			}
		}
	}

	return nil
}
