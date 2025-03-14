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

package chqk8smetricsconnector

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type md struct {
	id              component.ID
	config          *Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc

	emitterDone chan struct{}
}

func (c *md) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *md) Start(_ context.Context, host component.Host) error {
	// TODO set up http client here

	go c.emitter()

	return nil
}

func (c *md) emitter() {
	for {
		select {
		case <-c.emitterDone:
			return
		case <-time.Tick(c.config.Metrics.Interval):
			c.emitMetrics(time.Now())
		case <-time.Tick(c.config.Events.Interval):
			c.emitEvents(time.Now())
		}
	}
}

func (c *md) emitMetrics(now time.Time) {
	// md := c.buildMetrics(emitList)
	// if md.DataPointCount() == 0 {
	// 	return
	// }
	// if err := c.metricsConsumer.ConsumeMetrics(context.Background(), md); err != nil {
	// 	c.logger.Error("failed to emit metrics", zap.Error(err))
	// }
}

func (c *md) emitEvents(now time.Time) {
}

func (c *md) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	// TODO implement log to metrics conversion
	return nil
}
