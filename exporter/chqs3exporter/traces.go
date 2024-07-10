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
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func (e *s3Exporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	var ee translate.Environment
	if e.idsFromEnv {
		ee = translate.EnvironmentFromEnv()
	} else {
		ee = EnvironmentFromAuth(ctx)
	}

	if e.config.Timeboxes.Traces.Interval <= 0 {
		return nil
	}

	tbl, err := e.tb.TracesFromOtel(&traces, ee)
	if err != nil {
		return err
	}

	now := time.Now()
	interval := e.boxer.IntervalForTime(now)
	custmap := e.partitionTableByCustomerID(interval, tbl)
	return e.writeTableByCustomerID(now, custmap)
}
