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
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter/internal/metadata"
)

type exporterTelemetry struct {
	filesWritten    metric.Int64Counter
	datapointTooOld metric.Int64Counter
}

func newTelemetry(set exporter.Settings) (*exporterTelemetry, error) {
	tel := &exporterTelemetry{}

	counter, err := metadata.Meter(set.TelemetrySettings).Int64Counter(
		"exporter_"+metadata.Type.String()+"_files_written",
		metric.WithDescription("The total number of files written by the exporter"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.filesWritten = counter

	counter, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		"exporter_"+metadata.Type.String()+"_datapoint_too_old",
		metric.WithDescription("The total number of datapoints that are too old to be written by the exporter"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.datapointTooOld = counter

	return tel, nil
}
