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

package chqstatsexporter

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type statsExporter struct {
	config *Config
	logger *zap.Logger
}

func newStatsExporter(config *Config, params exporter.CreateSettings) *statsExporter {
	statsExporter := &statsExporter{
		config: config,
		logger: params.Logger,
	}
	return statsExporter
}

func (e *statsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *statsExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	return nil
}

func (e *statsExporter) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	return nil
}

func (e *statsExporter) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	return nil
}
