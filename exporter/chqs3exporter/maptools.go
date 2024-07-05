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
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/hashicorp/go-multierror"
	"go.uber.org/zap"
)

var (
	foundCustomerId = ""
	foundOnce       = sync.Once{}
	foundClusterId  = ""
)

func getIDs() {
	foundCustomerId = os.Getenv("CARDINALHQ_CUSTOMER_ID")
	if foundCustomerId == "" {
		foundCustomerId = "_default"
	}
	foundClusterId = os.Getenv("CARDINALHQ_CLUSTER_ID")
	if foundClusterId == "" {
		foundClusterId = "_default"
	}
}
func customerIDFromEnv() string {
	foundOnce.Do(func() {
		getIDs()
	})
	return foundCustomerId
}

func clusterIDFromEnv() string {
	foundOnce.Do(func() {
		getIDs()
	})
	return foundClusterId
}

// customerIDFromMap extracts a customer ID from a map.
// If the customer ID is not found or is not a string, it returns an empty string.
func customerIDFromMap(m map[string]any) string {
	customerID := customerIDFromEnv()
	if customerID != "_default" {
		return customerID
	}
	id, found := m[translate.CardinalFieldCustomerID]
	if found {
		if cid, ok := id.(string); ok {
			return cid
		}
	}
	return "_default"
}

func getClusterIDFromMap(m map[string]any) string {
	clusterID := clusterIDFromEnv()
	if clusterID != "_default" {
		return clusterID
	}
	id, found := m[translate.CardinalFieldClusterID]
	if found {
		if cid, ok := id.(string); ok {
			return cid
		}
	}
	return "_default"
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

func keyFromMap(m map[string]any) string {
	cid := customerIDFromMap(m)
	clid := getClusterIDFromMap(m)
	return fmt.Sprintf("%s/%s", cid, clid)
}

func (e *s3Exporter) partitionTableByCustomerID(interval int64, tbl []map[string]any) map[string][]map[string]any {
	custmap := map[string][]map[string]any{}
	for _, log := range tbl {
		key := keyFromMap(log)
		custmap[key] = append(custmap[key], log)
		if err := e.updateTagMap(key, interval, log); err != nil {
			e.logger.Error("failed to update tag map", zap.Error(err))
		}
	}
	return custmap
}

func (e *s3Exporter) partitionTableByCustomerIDAndInterval(tbl []map[string]any) map[string]map[int64][]map[string]any {
	custmap := make(map[string]map[int64][]map[string]any)
	for _, m := range tbl {
		key := keyFromMap(m)
		ts, found := timestampFromMap(m)
		if !found {
			ts = time.Now()
		}
		interval := e.boxer.IntervalForTime(ts)
		if _, ok := custmap[key]; !ok {
			custmap[key] = make(map[int64][]map[string]any)
		}
		if _, ok := custmap[key][interval]; !ok {
			custmap[key][interval] = make([]map[string]any, 0)
		}
		custmap[key][interval] = append(custmap[key][interval], m)
		if err := e.updateTagMap(key, interval, m); err != nil {
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
	var errs *multierror.Error
	for cid, intervals := range tbl {
		for interval, metrics := range intervals {
			ts := e.boxer.TimeForInterval(interval)
			errs = multierror.Append(errs, e.writeTableForCustomerID(cid, ts, metrics))
		}
	}
	return errs.ErrorOrNil()
}

func splitCustomerID(ids string) (string, string) {
	parts := strings.Split(ids, "/")
	cid, clid := "_default", "_default"
	if len(parts) > 0 {
		cid = parts[0]
	}
	if len(parts) > 1 {
		clid = parts[1]
	}
	return cid, clid
}

func (e *s3Exporter) writeTableForCustomerID(ids string, now time.Time, tbl []map[string]any) error {
	customerID, clusterID := splitCustomerID(ids)
	if len(tbl) == 0 {
		e.logger.Debug("no items to put to store", zap.String("customerID", customerID), zap.String("clusterID", clusterID), zap.Time("timestamp", now))
	}
	// validate the customer ID
	for _, item := range tbl {
		cid := customerIDFromMap(item)
		if customerID != cid {
			return fmt.Errorf("customer ID mismatch: %s != %s", customerID, cid)
		}
	}

	b, err := gobEncode(tbl)
	if err != nil {
		return err
	}
	tooOld, err := e.boxer.Put(ids, now, b)
	if err != nil {
		e.logger.Error("failed to put items to KVS", zap.Error(err))
		return err
	}
	e.logger.Debug("put items to store",
		zap.String("customerID", customerID),
		zap.String("clusterID", clusterID),
		zap.Time("timestamp", now),
		zap.Int("count", len(tbl)),
		zap.Bool("tooOld", tooOld),
		zap.Int64("interval", e.boxer.IntervalForTime(now)))
	return nil
}
