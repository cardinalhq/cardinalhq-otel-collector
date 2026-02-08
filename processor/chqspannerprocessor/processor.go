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

package chqspannerprocessor

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqspannerprocessor/internal/metadata"
)

type chqspanner struct {
	logger             *zap.Logger
	id                 component.ID
	injectedSpansCount metric.Int64Counter
}

func newSpanner(_ *Config, set processor.Settings) (*chqspanner, error) {
	p := &chqspanner{
		id:     set.ID,
		logger: set.Logger,
	}

	c, err := metadata.Meter(set.TelemetrySettings).Int64Counter(
		"cardinalhq.chqspanner.injected_spans",
		metric.WithDescription("Number of spans injected by the CHQ Spanner processor"),
	)
	if err != nil {
		return nil, err
	}
	p.injectedSpansCount = c

	return p, nil
}

func (p *chqspanner) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
