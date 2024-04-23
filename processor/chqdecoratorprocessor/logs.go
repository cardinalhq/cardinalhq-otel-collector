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

package chqdecoratorprocessor

import (
	"context"
	"fmt"

	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/fingerprinter"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type filterLogProcessor struct {
	telemetry *decoratorProcessorTelemetry
	logger    *zap.Logger
}

func newDecoratorLogsProcessor(set processor.CreateSettings, _ *Config) (*filterLogProcessor, error) {
	var err error
	dsp := &filterLogProcessor{
		logger: set.Logger,
	}

	dpt, err := newDecoratorProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	dsp.telemetry = dpt

	set.Logger.Info(
		"Decorator processor configured",
	)

	return dsp, nil
}

func (dmp *filterLogProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)

				_, level := fingerprinter.Fingerprint(log.Body().AsString())
				// log.Attributes().PutInt("cardinalhq._fingerprint", fingerprint)
				log.Attributes().PutStr("cardinalhq._level", level)
				log.Attributes().PutStr("cardinalhq.was", "here")
			}
		}
	}

	return ld, nil
}
