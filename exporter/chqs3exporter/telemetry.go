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

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/metadata"
)

type exporterTelemetry struct {
	datapointAge metric.Float64Histogram
}

func newTelemetry(set exporter.Settings) (*exporterTelemetry, error) {
	tel := &exporterTelemetry{}

	hg, err := metadata.Meter(set.TelemetrySettings).Float64Histogram(
		"datapoint_age",
		metric.WithDescription("The age of datapoints that are being written by the exporter"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	tel.datapointAge = hg

	return tel, nil
}

func recordAge(ctx context.Context, tel *exporterTelemetry, age float64, aset attribute.Set) {
	tel.datapointAge.Record(ctx, age, metric.WithAttributeSet(aset))
}
