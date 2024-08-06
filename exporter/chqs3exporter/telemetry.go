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
	datapointAge      metric.Float64Histogram
	itemsWrittenTemp  metric.Int64Counter
	blocksWrittenTemp metric.Int64Counter
	itemsReadTemp     metric.Int64Counter
	blocksReadTemp    metric.Int64Counter
	deltaBlocksRead   metric.Int64Counter

	aset attribute.Set
}

func newTelemetry(set exporter.Settings, ttype string) (*exporterTelemetry, error) {
	tel := &exporterTelemetry{}

	aset := attribute.NewSet(
		attribute.String("exporter", "chqs3exporter"),
		attribute.String("component.type", "exporter"),
		attribute.String("component.id", set.ID.String()),
		attribute.String("telemetry_type", ttype),
	)
	tel.aset = aset

	hg, err := metadata.Meter(set.TelemetrySettings).Float64Histogram(
		"datapoint_age",
		metric.WithDescription("The age of datapoints that are being written by the exporter"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	tel.datapointAge = hg

	ic, err := metadata.Meter(set.TelemetrySettings).Int64Counter(
		"items_written_temp",
		metric.WithDescription("The number of items written by the exporter to temporary storage"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.itemsWrittenTemp = ic

	ic, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		"blocks_written_temp",
		metric.WithDescription("The number of blocks written by the exporter to temporary storage"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.blocksWrittenTemp = ic

	ic, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		"items_read_temp",
		metric.WithDescription("The number of items read by the exporter from temporary storage"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.itemsReadTemp = ic

	ic, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		"blocks_read_temp",
		metric.WithDescription("The number of blocks read by the exporter from temporary storage"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.blocksReadTemp = ic

	ic, err = metadata.Meter(set.TelemetrySettings).Int64Counter(
		"delta_blocks_read",
		metric.WithDescription("The number of blocks read by the exporter from temporary storage vs the expected value"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	tel.deltaBlocksRead = ic

	return tel, nil
}

func recordAge(ctx context.Context, tel *exporterTelemetry, age float64) {
	tel.datapointAge.Record(ctx, age, metric.WithAttributeSet(tel.aset))
}
