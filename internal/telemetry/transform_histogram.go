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

package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

type DeferrableInt64Histogram struct {
	histogram     metric.Int64Histogram
	recordOptions []metric.RecordOption
}

func (d DeferrableInt64Histogram) Record(delta int64, options ...metric.RecordOption) {
	d.histogram.Record(context.Background(), delta, append(d.recordOptions, options...)...)
}

var _ DeferrableHistogram = (*DeferrableInt64Histogram)(nil)

func NewDeferrableHistogram(meter metric.Meter, name string, histogramOptions []metric.Int64HistogramOption, recordOptions []metric.RecordOption) (*DeferrableInt64Histogram, error) {
	histogram, err := meter.Int64Histogram(name, histogramOptions...)
	if err != nil {
		return nil, err
	}
	return &DeferrableInt64Histogram{
		histogram:     histogram,
		recordOptions: recordOptions,
	}, nil
}
