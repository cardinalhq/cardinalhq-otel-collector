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

	"github.com/cardinalhq/oteltools/signalbuilder"
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

	converters map[objectSelector]converterFunc

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
		case <-time.Tick(c.config.Events.Interval):
			c.emitEvents(time.Now())
		}
	}
}

func (c *md) emitEvents(now time.Time) {
}

func (c *md) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	builder := signalbuilder.NewMetricsBuilder()

	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)
			for k := range sl.LogRecords().Len() {
				lr := sl.LogRecords().At(k)
				c.processObject(builder, rl.Resource().Attributes(), lr.Attributes(), lr.Body())
			}
		}
	}

	out := builder.Build()
	if out.DataPointCount() == 0 {
		return nil
	}
	return c.metricsConsumer.ConsumeMetrics(context.Background(), out)
}

func build(rm *signalbuilder.MetricResourceBuilder, lr plog.LogRecord) {
	// TODO implement log to metrics conversion
}
