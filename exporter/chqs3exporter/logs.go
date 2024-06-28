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

	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/multierr"
)

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	var errs error
	oldestTimestamp, customerIDs, err := e.consumeLogs(time.Now().UnixMilli(), logs)
	errs = multierr.Append(errs, err)
	go e.writeClosed(customerIDs, oldestTimestamp, logFilePrefix, e.logs)
	return errs
}

func (e *s3Exporter) consumeLogs(now int64, logs plog.Logs) (int64, []string, error) {
	if e.config.Timeboxes.Logs.Interval <= 0 {
		return 0, nil, nil
	}
	return e.appendLogs(now, logs)
}

func (e *s3Exporter) appendLogs(now int64, ld plog.Logs) (int64, []string, error) {
	tbl, err := e.tb.LogsFromOtel(&ld)
	if err != nil {
		return 0, nil, err
	}
	return e.emitRows(now, true, tbl, e.logs, logFilePrefix)
}
