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

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
)

func (e *s3Exporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	if e.config.Timeboxes.Traces.Interval <= 0 {
		return nil
	}
	var errs error
	oldestTimestamp, customerIDs, err := e.appendTraces(time.Now().UnixMilli(), traces)
	errs = multierr.Append(errs, err)
	go e.writeClosed(customerIDs, oldestTimestamp, tracesFilePrefix, e.traces)
	return errs
}

func (e *s3Exporter) appendTraces(now int64, td ptrace.Traces) (int64, []string, error) {
	tbl, err := e.tb.TracesFromOtel(&td)
	if err != nil {
		return 0, nil, err
	}
	return e.emitRows(now, true, tbl, e.traces, tracesFilePrefix)
}
