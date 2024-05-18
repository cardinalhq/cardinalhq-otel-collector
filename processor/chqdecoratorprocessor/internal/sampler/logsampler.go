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
	"maps"
	"math/rand"
	"sync"

	"github.com/honeycombio/dynsampler-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

type LogSampler interface {
	Sample(fingerprint string, rattr pcommon.Map, iattr pcommon.Map, lattr pcommon.Map) (droppingRule string)
	UpdateConfig(config *SamplerConfig)
}

var _ LogSampler = (*LogSamplerImpl)(nil)

type LogSamplerImpl struct {
	sync.RWMutex
	rules map[string]logRule

	logger *zap.Logger
}

type logRule struct {
	id       string
	ruleType LogRuleType
	sampler  dynsampler.Sampler
	scope    map[string]string
}

func NewLogSamplerImpl(ctx context.Context, logger *zap.Logger) *LogSamplerImpl {
	ls := &LogSamplerImpl{
		logger: logger,
		rules:  map[string]logRule{},
	}
	return ls
}

func (ls *LogSamplerImpl) UpdateConfig(config *SamplerConfig) {
	ls.Lock()
	defer ls.Unlock()

	ls.configure(config.Logs.Sampling)
}

func (ls *LogSamplerImpl) Sample(fingerprint string, rattr pcommon.Map, iattr pcommon.Map, lattr pcommon.Map) (droppingRule string) {
	ls.RLock()
	defer ls.RUnlock()

	return ls.shouldSample(fingerprint, rattr, iattr, lattr)
}

func (ls *LogSamplerImpl) shouldSample(fingerprint string, rattr pcommon.Map, iattr pcommon.Map, lattr pcommon.Map) (droppingRule string) {
	ret := ""
	matched := false

	randval := rand.Float64()
	for rid, r := range ls.rules {
		// if we already have a rule, don't bother checking any more random rules.
		if matched && r.ruleType == LogRuleTypeRandom {
			continue
		}
		attrs := map[string]pcommon.Map{
			"resource":        rattr,
			"instrumentation": iattr,
			"log":             lattr,
		}
		if matchscope(r.scope, attrs) {
			rate := r.sampler.GetSampleRate(fingerprint)
			switch rate {
			case 0:
				if !matched {
					ret = rid
					matched = true
				}
			case 1:
				continue
			default:
				if randval <= rpsToRandom(rate) {
					if !matched {
						ret = rid
						matched = true
					}
				}
			}
		}
	}
	return ret
}

func rpsToRandom(rate int) float64 {
	return 1 / float64(rate)
}

func randomToRPS(rate float64) int {
	return int(1 / rate)
}

func (ls *LogSamplerImpl) configure(config []LogSamplingConfig) {
	currentIDs := map[string]bool{}
	for k := range ls.rules {
		currentIDs[k] = true
	}

	for _, c := range config {
		if c.RuleType != "random" && c.RuleType != "rps" {
			ls.logger.Error("Unknown log sampling rule type", zap.String("type", c.RuleType), zap.String("id", c.Id))
			continue
		}
		if currentrule, ok := ls.rules[c.Id]; ok {
			_ = currentrule.sampler.Stop()
			updateCurrentRule(currentrule, c)
		} else {
			ls.addRule(c)
		}
		delete(currentIDs, c.Id)

		if err := ls.rules[c.Id].sampler.Start(); err != nil {
			ls.logger.Error("Error starting log sampler", zap.Error(err), zap.String("id", c.Id), zap.String("type", c.RuleType))
		}
	}

	for k := range currentIDs {
		r := ls.rules[k]
		_ = r.sampler.Stop()
		delete(ls.rules, k)
	}
}

// new rule must be started by the caller.
func (ls *LogSamplerImpl) addRule(c LogSamplingConfig) {
	r := logRule{
		id:       c.Id,
		ruleType: logRuletypeToInt(c.RuleType),
		scope:    c.Scope,
	}
	r.ruleType, r.sampler = samplerForType(c)
	if r.sampler == nil {
		ls.logger.Error("Unknown log sampling rule type", zap.String("type", c.RuleType))
		return
	}
	if err := r.sampler.Start(); err != nil {
		ls.logger.Error("Error starting log sampler", zap.Error(err))
		return
	}
	ls.rules[c.Id] = r
}

func samplerForType(c LogSamplingConfig) (ruleType LogRuleType, sampler dynsampler.Sampler) {
	switch c.RuleType {
	case "random":
		return LogRuleTypeRandom, &dynsampler.Static{Default: randomToRPS(c.SampleRate)}
	case "rps":
		return LogRuleTypeRPS, &dynsampler.AvgSampleWithMin{GoalSampleRate: c.RPS}
	}
	return LogRuleTypeUnknown, nil
}

// existing rule must be stopped and started by the caller.
func updateCurrentRule(r logRule, c LogSamplingConfig) {
	cps := logRuletypeToInt(c.RuleType)
	if r.ruleType != cps {
		r.ruleType, r.sampler = samplerForType(c)
	}
	if !maps.Equal(c.Scope, r.scope) {
		r.scope = c.Scope
	}
}
