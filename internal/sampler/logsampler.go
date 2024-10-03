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

package sampler

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/plog"
	"math/rand"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

type LogSampler interface {
	Sample(serviceName string, fingerprint string, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) (droppingRule string)
	UpdateConfig(config *SamplerConfig, vendor string)
}

var _ LogSampler = (*LogSamplerImpl)(nil)

type LogSamplerImpl struct {
	sync.RWMutex
	rules map[string]*logRule

	logger *zap.Logger
}

func NewLogSamplerImpl(ctx context.Context, logger *zap.Logger) *LogSamplerImpl {
	ls := &LogSamplerImpl{
		logger: logger,
		rules:  map[string]*logRule{},
	}
	return ls
}

func (ls *LogSamplerImpl) UpdateConfig(config *SamplerConfig, vendor string) {
	ls.Lock()
	defer ls.Unlock()
	ls.configure(config.Logs.Sampling, vendor)
}

func (ls *LogSamplerImpl) Sample(serviceName string,
	fingerprint string,
	rl plog.ResourceLogs,
	sl plog.ScopeLogs,
	ll plog.LogRecord) (droppingRule string) {
	ls.RLock()
	defer ls.RUnlock()

	return ls.shouldFilter(serviceName, fingerprint, rl, sl, ll)
}

func getServiceName(rattr pcommon.Map) string {
	serviceName, ok := rattr.Get("service.name")
	if !ok {
		return "unknown-service"
	}
	return serviceName.AsString()
}

func (ls *LogSamplerImpl) shouldFilter(serviceName string,
	fingerprint string,
	rl plog.ResourceLogs,
	sl plog.ScopeLogs,
	ll plog.LogRecord) (droppingRule string) {
	if len(ls.rules) == 0 {
		return ""
	}

	ret := ""
	matched := false
	key := fmt.Sprintf("%s:%s", serviceName, fingerprint)
	randval := rand.Float64()

	for rid, r := range ls.rules {
		// if we already have a match, don't bother checking any more random rules.
		if matched && r.ruleType == LogRuleTypeRandom {
			continue
		}
		if r.evaluate(rl, sl, ll) {
			rate := r.sampler.GetSampleRate(key)
			wasHit := shouldFilter(rate, randval)
			if wasHit && !matched {
				ret = rid
				matched = true
			}
		}
	}

	return ret
}

func shouldFilter(rate int, randval float64) bool {
	switch rate {
	case 0:
		return true
	case 1:
		return false
	default:
		return randval > 1/float64(rate)
	}
}

func rpsToRandom(rate int) float64 {
	return 1 / float64(rate)
}

func randomToRPS(rate float64) int {
	return int(1 / rate)
}

func (ls *LogSamplerImpl) configure(config []LogSamplingConfigV1, vendor string) {
	ls.logger.Info("Updating log sampling rules", zap.Any("config", config))
	currentIDs := map[string]bool{}
	for k := range ls.rules {
		currentIDs[k] = true
	}

	for _, c := range config {
		if c.Vendor != vendor {
			continue
		}
		if c.RuleType != "random" && c.RuleType != "rps" {
			ls.logger.Error("Unknown log sampling rule type", zap.String("type", c.RuleType), zap.String("id", c.Id))
			continue
		}
		if currentrule, ok := ls.rules[c.Id]; ok {
			ls.updateCurrentRule(currentrule, c)
		} else {
			ls.addRule(c)
		}
		delete(currentIDs, c.Id)
	}

	// clean up any old rules
	for k := range currentIDs {
		r := ls.rules[k]
		_ = r.sampler.Stop()
		ls.logger.Info("Removing log sampling rule", zap.String("id", k))
		delete(ls.rules, k)
	}
}

// new rule must be started by the caller.
func (ls *LogSamplerImpl) addRule(c LogSamplingConfigV1) {
	ls.logger.Info("Adding log sampling rule", zap.String("id", c.Id), zap.Any("config", c))
	r := newLogRule(c)

	r.ruleType, r.sampler = samplerForType(c, ls.logger)
	if r.sampler == nil {
		ls.logger.Error("Unknown log sampling rule type", zap.String("type", c.RuleType))
		return
	}
	if err := r.sampler.Start(); err != nil {
		ls.logger.Error("Error starting log sampler", zap.Error(err))
		return
	}
	ls.logger.Info("Started log sampling rule", zap.String("id", c.Id))
	ls.rules[c.Id] = r
}

func samplerForType(c LogSamplingConfigV1, logger *zap.Logger) (ruleType LogRuleType, sampler Sampler) {
	switch c.RuleType {
	case "random":
		return LogRuleTypeRandom, NewStaticSampler(int(1 / c.SampleRate))
	case "rps":
		switch c.RPS {
		case 0, 1:
			return LogRuleTypeRPS, NewStaticSampler(c.RPS)
		default:
			return LogRuleTypeRPS, NewRPSSampler(WithMaxRPS(c.RPS), WithLogger(logger))
		}
	}
	return LogRuleTypeUnknown, nil
}

// existing rule must be stopped and started by the caller.
func (ls *LogSamplerImpl) updateCurrentRule(r *logRule, c LogSamplingConfigV1) {
	if r.config.Equals(c) {
		return
	}
	ls.logger.Info("Updating log sampling rule", zap.String("id", c.Id), zap.Any("config", c))
	_ = r.sampler.Stop()
	r.ruleType, r.sampler = samplerForType(c, ls.logger)
	if r.sampler == nil {
		ls.logger.Error("Unknown log sampling rule type", zap.String("type", c.RuleType))
		return
	}
	r.config = c
	r.parseConditions()

	if err := r.sampler.Start(); err != nil {
		ls.logger.Error("Error starting log sampler", zap.Error(err))
	}
	ls.logger.Info("Started log sampling rule", zap.String("id", c.Id))
}
