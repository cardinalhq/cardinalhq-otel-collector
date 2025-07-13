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
	"go.uber.org/zap"
)

type chqspanner struct {
	logger            *zap.Logger
	id                component.ID
	nextTraceReceiver consumer.Traces
}

func newSpanner(_ *Config, set processor.Settings, nextConsumer consumer.Traces) (*chqspanner, error) {
	p := &chqspanner{
		id:                set.ID,
		logger:            set.Logger,
		nextTraceReceiver: nextConsumer,
	}

	return p, nil
}

func (p *chqspanner) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
