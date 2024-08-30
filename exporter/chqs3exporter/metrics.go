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

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
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

	e.logger.Debug("ConsumeMetrics", zap.String("customerID", ee.CustomerID()), zap.Int("datapoints-in", md.DataPointCount()))

	tbl, err := e.tb.MetricsFromOtel(&md, ee)
	if err != nil {
		return err
	}

	e.logger.Debug("ConsumeMetrics", zap.String("customerID", ee.CustomerID()), zap.Int("table-size", len(tbl)))

	custmap := e.partitionTableByCustomerIDAndInterval(tbl, e.config.UseNowForMetrics)
	return e.writeTableByCustomerIDAndInterval(custmap)
}
