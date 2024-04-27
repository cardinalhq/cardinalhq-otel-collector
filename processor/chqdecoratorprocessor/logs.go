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

	if conf.SamplerConfigFile != "" {
		confmgr, err := makeConfigurationManager(conf, set.Logger)
		if err != nil {
			return nil, fmt.Errorf("error creating configuration manager: %w", err)
		}
		go confmgr.Run()
		dsp.configManager = confmgr

		dsp.updaterId = confmgr.RegisterCallback("logsampler", func(config sampler.SamplerConfig) {
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
	dropped := int64(0)
	processed := int64(0)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level := fingerprinter.Fingerprint(log.Body().AsString())
				log.Attributes().PutInt("_cardinalhq.fingerprint", fingerprint)
				log.Attributes().PutStr("_cardinalhq.level", level)
				rule_match := dmp.sampler.Sample(rl.Resource().Attributes(), sl.Scope().Attributes(), log.Attributes())
				log.Attributes().PutStr("_cardinalhq.rule_match", rule_match)
				log.Attributes().PutBool("_cardinalhq.filtered", rule_match != "")
				processed++
				if rule_match != "" {
					dropped++
				}
			}
		}
	}

	dmp.telemetry.record(triggerLogsDropped, dropped)
	dmp.telemetry.record(triggerLogsProcessed, processed)

	return ld, nil
}

func (dmp *filterLogProcessor) Shutdown(context.Context) error {
	if dmp.configManager != nil {
		dmp.configManager.UnregisterCallback(dmp.updaterId)
		dmp.configManager.Stop()
	}
	return nil
}
