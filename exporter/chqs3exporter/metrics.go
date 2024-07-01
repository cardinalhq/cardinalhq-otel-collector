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
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if e.config.Timeboxes.Metrics.Interval <= 0 {
		return nil
	}

	tbl, err := e.tb.MetricsFromOtel(&md)
	if err != nil {
		return err
	}

	custmap := e.partitionTableByCustomerIDAndInterval(tbl)
	return e.writeTableByCustomerIDAndInterval(custmap)
}

func timestampFromMap(m map[string]any) time.Time {
	ts, ok := m[translate.CardinalFieldTimestamp]
	if !ok {
		return time.Now() // should never happen
	}
	tsMillis, ok := ts.(int64)
	if !ok {
		return time.Now() // should never happen
	}
	return time.UnixMilli(tsMillis)
}

func (e *s3Exporter) partitionTableByCustomerIDAndInterval(tbl []map[string]any) map[string]map[int64][]map[string]any {
	custmap := make(map[string]map[int64][]map[string]any)
	for _, m := range tbl {
		cid := customerIDFromMap(m)
		ts := timestampFromMap(m)
		interval := e.boxer.IntervalForTime(ts)
		if _, ok := custmap[cid]; !ok {
			custmap[cid] = make(map[int64][]map[string]any)
		}
		if _, ok := custmap[cid][interval]; !ok {
			custmap[cid][interval] = make([]map[string]any, 0)
		}
		custmap[cid][interval] = append(custmap[cid][interval], m)
		if err := e.updateTagMap(cid, interval, m); err != nil {
			e.logger.Error("failed to update tag map", zap.Error(err))
		}
	}
	return custmap
}

func (e *s3Exporter) writeTableByCustomerIDAndInterval(tbl map[string]map[int64][]map[string]any) error {
	var errs error
	for cid, intervals := range tbl {
		for interval, metrics := range intervals {
			ts := e.boxer.TimeForInterval(interval)
			errs = multierr.Append(errs, e.writeTableForCustomerID(cid, ts, metrics))
		}
	}
	return errs
}
