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
	"encoding/json"
	"time"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	if e.config.Timeboxes.Logs.Interval <= 0 {
		return nil
	}

	tbl, err := e.tb.LogsFromOtel(&logs)
	if err != nil {
		return err
	}

	now := time.Now()
	interval := e.boxer.IntervalForTime(now)
	custmap := e.partitionByCustomerID(interval, tbl)
	return e.writeTableByCustomerID(now, custmap)
}

func (e *s3Exporter) writeTableByCustomerID(now time.Time, tbl map[string][]map[string]any) error {
	var errs *multierror.Error
	for customerID, logs := range tbl {
		errs = multierror.Append(errs, e.writeTableForCustomerID(customerID, now, logs))
	}
	return errs.ErrorOrNil()
}

func (e *s3Exporter) writeTableForCustomerID(customerID string, now time.Time, tbl []map[string]any) error {
	b, err := json.Marshal(tbl)
	if err != nil {
		return err
	}
	if _, err := e.boxer.Put(customerID, now, b); err != nil {
		return err
	}
	return nil
}

func (e *s3Exporter) partitionByCustomerID(interval int64, tbl []map[string]any) map[string][]map[string]any {
	custmap := map[string][]map[string]any{}
	for _, log := range tbl {
		customerID := customerIDFromMap(log)
		custmap[customerID] = append(custmap[customerID], log)
		if err := e.updateTagMap(customerID, interval, log); err != nil {
			e.logger.Error("failed to update tag map", zap.Error(err))
		}
	}
	return custmap
}
