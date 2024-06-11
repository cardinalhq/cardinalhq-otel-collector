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
	"strconv"
	"strings"
	"sync"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor/internal/sampler"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

type logProcessor struct {
	telemetry     *processorTelemetry
	sampler       sampler.LogSampler
	logger        *zap.Logger
	configManager sampler.ConfigManager
	updaterId     int
	finger        fingerprinter.Fingerprinter
}

func newLogsProcessor(set processor.CreateSettings, conf *Config) (*logProcessor, error) {
	samp := sampler.NewLogSamplerImpl(context.Background(), set.Logger)

	lp := &logProcessor{
		logger:  set.Logger,
		sampler: samp,
	}

	if conf.SamplerConfigFile != "" {
		confmgr, err := makeConfigurationManager(conf, set.Logger)
		if err != nil {
			return nil, fmt.Errorf("error creating configuration manager: %w", err)
		}
		go confmgr.Run()
		lp.configManager = confmgr

		lp.updaterId = confmgr.RegisterCallback("logsampler", func(config sampler.SamplerConfig) {
			samp.UpdateConfig(&config)
		})
	}

	dpt, err := newProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	lp.telemetry = dpt

	set.Logger.Info(
		"Decorator processor configured",
	)

	lp.finger = fingerprinter.NewFingerprinter()

	return lp, nil
}

func getServiceName(rattr pcommon.Map) string {
	if serviceName, ok := rattr.Get("service.name"); ok {
		return serviceName.AsString()
	}
	return "unknown-service"
}

var (
	newFingerprintLock sync.Mutex
	fingerprints       = map[int64]int64{}
)

func addFingerprint(fingerprint int64) int64 {
	newFingerprintLock.Lock()
	defer newFingerprintLock.Unlock()
	val := fingerprints[fingerprint]
	fingerprints[fingerprint] = val + 1
	return val
}

func (lp *logProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	filtered := int64(0)
	processed := int64(0)
	filteredKeys := map[string]int64{}
	processedKeys := map[string]int64{}
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				serviceName := getServiceName(rl.Resource().Attributes())
				fingerprint, level, err := lp.finger.Fingerprint(log.Body().AsString())
				if err != nil {
					lp.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}
				if addFingerprint(fingerprint) == 0 {
					lp.logger.Debug("New fingerprint",
						zap.Int64("fingerprint", fingerprint),
						zap.String("service", serviceName),
						zap.String("level", level))
				}
				fingerprintString := fmt.Sprintf("%d", fingerprint)
				log.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				log.Attributes().PutStr(translate.CardinalFieldLevel, level)
				rule_match := lp.sampler.Sample(fingerprintString, rl.Resource().Attributes(), sl.Scope().Attributes(), log.Attributes())
				wasFiltered := rule_match != ""
				log.Attributes().PutStr(translate.CardinalFieldRuleMatch, rule_match)
				log.Attributes().PutBool(translate.CardinalFieldFiltered, wasFiltered)
				log.Attributes().PutBool(translate.CardinalFieldWouldFilter, wasFiltered)
				key := fmt.Sprintf("%s:%d", serviceName, fingerprint)
				processed++
				processedKeys[key]++
				if wasFiltered {
					lp.logger.Info("Log filtered", zap.String("key", key))
					filtered++
					filteredKeys[key]++
				}
			}
		}
	}

	for key, count := range processedKeys {
		filtered := filteredKeys[key]
		items := strings.Split(key, ":")
		serviceName := items[0]
		fingerprint, _ := strconv.ParseInt(items[1], 10, 64)
		if count-filtered > 0 {
			lp.telemetry.record(triggerLogsProcessed, count-filtered,
				attribute.Bool("filtered.filtered", false),
				attribute.String("filtered.classification", "not_filtered"),
				attribute.String("filtered.service.name", serviceName),
				attribute.Int64("filtered.fingerprint", fingerprint))
		}
		if filtered > 0 {
			lp.telemetry.record(triggerLogsProcessed, filtered,
				attribute.Bool("filtered.filtered", true),
				attribute.String("filtered.classification", "rule_match"),
				attribute.String("filtered.service.name", serviceName),
				attribute.Int64("filtered.fingerprint", fingerprint))
		}
	}

	return ld, nil
}

func (lp *logProcessor) Shutdown(_ context.Context) error {
	if lp.configManager != nil {
		lp.configManager.UnregisterCallback(lp.updaterId)
		lp.configManager.Stop()
	}
	return nil
}
