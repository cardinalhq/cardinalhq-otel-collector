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
	"github.com/cardinalhq/otel-collector-saas/processor/chqdecoratorprocessor/internal/sampler"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type filterLogProcessor struct {
	telemetry     *decoratorProcessorTelemetry
	sampler       sampler.LogSampler
	logger        *zap.Logger
	configManager sampler.ConfigManager
	updaterId     int
}

func newDecoratorLogsProcessor(set processor.CreateSettings, conf *Config) (*filterLogProcessor, error) {
	samp := sampler.NewLogSamplerImpl(context.Background(), set.Logger)

	dsp := &filterLogProcessor{
		logger:  set.Logger,
		sampler: samp,
	}

	if conf.SampleConfigFile != "" {
		confmgr, err := makeConfigurationManager(conf, set.Logger)
		if err != nil {
			return nil, fmt.Errorf("error creating configuration manager: %w", err)
		}
		go confmgr.Run()
		dsp.configManager = confmgr

		dsp.updaterId = confmgr.RegisterCallback("sampler", func(config sampler.SamplerConfig) {
			samp.UpdateConfig(&config)
		})
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

func (dmp *filterLogProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level := fingerprinter.Fingerprint(log.Body().AsString())
				log.Attributes().PutInt("cardinalhq._fingerprint", fingerprint)
				log.Attributes().PutStr("cardinalhq._level", level)
				rule_match := dmp.sampler.Sample(rl.Resource().Attributes(), sl.Scope().Attributes(), log.Attributes())
				log.Attributes().PutStr("cardinalhq._rule_match", rule_match)
				log.Attributes().PutBool("cardinalhq._filtered", rule_match != "")
				if rule_match != "" {
					dmp.telemetry.record(triggerLogsDropped, 1)
				}
			}
		}
	}

	return ld, nil
}