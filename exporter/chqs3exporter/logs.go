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

package chqs3exporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	var ee translate.Environment
	if e.idsFromEnv {
		ee = translate.EnvironmentFromEnv()
	} else {
		ee = translate.EnvironmentFromAuth(ctx)
	}

	if e.config.Timeboxes.Logs.Interval <= 0 {
		return nil
	}

	tbl, err := e.tb.LogsFromOtel(&logs, ee)
	if err != nil {
		return err
	}

	now := time.Now()
	interval := e.boxer.IntervalForTime(now)
	custmap := e.partitionTableByCustomerID(interval, tbl)
	return e.writeTableByCustomerID(now, custmap)
}
