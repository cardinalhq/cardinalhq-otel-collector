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
	"encoding/json"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/hashicorp/go-multierror"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// customerIDFromMap extracts a customer ID from a map.
// If the customer ID is not found or is not a string, it returns an empty string.
func customerIDFromMap(m map[string]any) string {
	customerID, found := m[translate.CardinalFieldCustomerID]
	if !found {
		return ""
	}
	if cid, ok := customerID.(string); ok {
		return cid
	}
	return ""
}

// timestampFromMap extracts a timestamp from a map.
// If the timestamp is not found or is not an int64, it returns false.
func timestampFromMap(m map[string]any) (time.Time, bool) {
	ts, ok := m[translate.CardinalFieldTimestamp]
	if !ok {
		return time.Time{}, false
	}
	tsMillis, ok := ts.(int64)
	if !ok {
		return time.Time{}, false
	}
	return time.UnixMilli(tsMillis), true
}

func (e *s3Exporter) partitionTableByCustomerID(interval int64, tbl []map[string]any) map[string][]map[string]any {
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

func (e *s3Exporter) partitionTableByCustomerIDAndInterval(tbl []map[string]any) map[string]map[int64][]map[string]any {
	custmap := make(map[string]map[int64][]map[string]any)
	for _, m := range tbl {
		cid := customerIDFromMap(m)
		ts, found := timestampFromMap(m)
		if !found {
			ts = time.Now()
		}
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

func (e *s3Exporter) writeTableByCustomerID(now time.Time, tbl map[string][]map[string]any) error {
	var errs *multierror.Error
	for customerID, logs := range tbl {
		errs = multierror.Append(errs, e.writeTableForCustomerID(customerID, now, logs))
	}
	return errs.ErrorOrNil()
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
