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

package chqenforcerprocessor

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

type chqEnforcer struct {
	config     *Config
	httpClient *http.Client

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	logstats    *stats.StatsCombiner[*chqpb.LogStats]
	metricstats *stats.StatsCombiner[*MetricStat]

	logger *zap.Logger

	pbPhase chqpb.Phase
}

func newCHQEnforcer(config *Config, set processor.Settings) *chqEnforcer {
	now := time.Now()
	statsExporter := &chqEnforcer{
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logstats:           stats.NewStatsCombiner[*chqpb.LogStats](now, config.Interval),
		metricstats:        stats.NewStatsCombiner[*MetricStat](now, config.Interval),
		logger:             set.Logger,
	}
	if config.Phase == "presample" {
		statsExporter.pbPhase = chqpb.Phase_PRE
	} else {
		statsExporter.pbPhase = chqpb.Phase_POST
	}

	return statsExporter
}

func (e *chqEnforcer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (e *chqEnforcer) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient
	return nil
}
