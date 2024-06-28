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

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"
)

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var errs error
	oldestTimestamp, customerIDs, err := e.consumeMetrics(time.Now().UnixMilli(), md)
	errs = multierr.Append(errs, err)
	go e.writeClosed(customerIDs, oldestTimestamp, metricFilePrefix, e.metrics)
	return errs
}

func (e *s3Exporter) consumeMetrics(now int64, md pmetric.Metrics) (int64, []string, error) {
	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return 0, nil, nil
	}
	return e.appendMetrics(now, md)
}

func (e *s3Exporter) appendMetrics(now int64, md pmetric.Metrics) (int64, []string, error) {
	tbl, err := e.tb.MetricsFromOtel(&md)
	if err != nil {
		return 0, nil, err
	}
	return e.emitRows(now, false, tbl, e.metrics, metricFilePrefix)
}
