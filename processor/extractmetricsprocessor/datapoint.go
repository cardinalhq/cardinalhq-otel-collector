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

package extractmetricsprocessor

import (
	"fmt"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func updateDatapoint(ty string, name string, units string, builder *signalbuilder.MetricScopeBuilder, val float64, timestamp pcommon.Timestamp, attrs pcommon.Map) error {
	switch ty {
	case gaugeDoubleType, gaugeIntType:
		metric, err := builder.Metric(name, units, pmetric.MetricTypeGauge)
		if err != nil {
			return fmt.Errorf("failed when creating metric: %v", err)
		}
		updateDatapointGauge(metric, val, attrs, timestamp)
	case counterDoubleType, counterIntType:
		metric, err := builder.Metric(name, units, pmetric.MetricTypeSum)
		if err != nil {
			return fmt.Errorf("failed when creating metric: %v", err)
		}
		updateDatapointSum(metric, val, attrs, timestamp)
	}
	return nil
}

func updateDatapointSum(builder signalbuilder.MetricDatapointBuilder, val float64, attrs pcommon.Map, timestamp pcommon.Timestamp) {
	dp, _, isNew := builder.Datapoint(attrs, timestamp)
	if isNew {
		dp.SetDoubleValue(val)
	} else {
		dp.SetDoubleValue(dp.DoubleValue() + val)
	}
}

func updateDatapointGauge(builder signalbuilder.MetricDatapointBuilder, val float64, attrs pcommon.Map, timestamp pcommon.Timestamp) {
	dp, _, isNew := builder.Datapoint(attrs, timestamp)
	if isNew {
		dp.SetDoubleValue(val)
	} else {
		if dp.DoubleValue() < val {
			dp.SetDoubleValue(val)
		}
	}
}
