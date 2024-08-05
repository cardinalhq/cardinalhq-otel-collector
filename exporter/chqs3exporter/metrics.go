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

package chqs3exporter

import (
	"context"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var ee translate.Environment
	if e.idsFromEnv {
		ee = translate.EnvironmentFromEnv()
	} else {
		ee = EnvironmentFromAuth(ctx)
	}

	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return nil
	}

	e.calcMetricAge(md, time.Now())

	tbl, err := e.tb.MetricsFromOtel(&md, ee)
	if err != nil {
		return err
	}

	custmap := e.partitionTableByCustomerIDAndInterval(tbl)
	return e.writeTableByCustomerIDAndInterval(custmap)
}

func (e *s3Exporter) calcMetricAge(md pmetric.Metrics, now time.Time) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			imm := rm.ScopeMetrics().At(j)
			for k := 0; k < imm.Metrics().Len(); k++ {
				metric := imm.Metrics().At(k)
				var ts time.Time
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					ts = metric.Gauge().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeSum:
					ts = metric.Sum().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeHistogram:
					ts = metric.Histogram().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeSummary:
					ts = metric.Summary().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeExponentialHistogram:
					ts = metric.ExponentialHistogram().DataPoints().At(0).Timestamp().AsTime()
				}
				age := now.Sub(ts)
				recordAge(context.Background(), e.telemetry, age.Seconds())
			}
		}
	}
}
