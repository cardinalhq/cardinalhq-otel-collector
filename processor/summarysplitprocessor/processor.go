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

package summarysplitprocessor

import (
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/summarysplitprocessor/internal/metadata"
)

type summarysplit struct {
	logger     *zap.Logger
	splitCount metric.Int64Counter
}

func newSummarySplitter(_ *Config, set processor.Settings, _ consumer.Metrics) (*summarysplit, error) {
	splitCount, err := metadata.Meter(set.TelemetrySettings).Int64Counter(
		"summarysplit_summary_datapoints_split",
		metric.WithDescription("Number of summary metric data points split into count, sum, and quantile metrics"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	p := &summarysplit{
		logger:     set.Logger,
		splitCount: splitCount,
	}

	p.logger.Info("SummarySplit processor enabled. Splitting summary metrics into count, sum, and quantile metrics.")

	return p, nil
}

func (p *summarysplit) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
