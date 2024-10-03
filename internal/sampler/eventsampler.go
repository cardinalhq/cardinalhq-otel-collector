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
	"go.opentelemetry.io/collector/pdata/ptrace"
	"math/rand"
	"sync"

	"go.uber.org/zap"
)

type EventSampler interface {
	SampleLogs(serviceName string, fingerprint string, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) (droppingRule string)
	SampleSpans(serviceName string, fingerprint string, rl ptrace.ResourceSpans, sl ptrace.ScopeSpans, lr ptrace.Span) (droppingRule string)
	UpdateConfig(config []EventSamplingConfigV1, vendor string)
}

var _ EventSampler = (*EventSamplerImpl)(nil)

type EventSamplerImpl struct {
	sync.RWMutex
	rules map[string]*filterRule

	logger *zap.Logger
}

func NewEventSamplerImpl(ctx context.Context, logger *zap.Logger) *EventSamplerImpl {
	ls := &EventSamplerImpl{
		logger: logger,
		rules:  map[string]*filterRule{},
	}
	return ls
}

func (ls *EventSamplerImpl) UpdateConfig(config []EventSamplingConfigV1, vendor string) {
	ls.Lock()
	defer ls.Unlock()
	ls.configure(config, vendor)
}

func (ls *EventSamplerImpl) SampleLogs(serviceName string,
	fingerprint string,
	rl plog.ResourceLogs,
	sl plog.ScopeLogs,
	ll plog.LogRecord) (droppingRule string) {
	ls.RLock()
	defer ls.RUnlock()

	return ls.shouldFilterLog(serviceName, fingerprint, rl, sl, ll)
}

func (ls *EventSamplerImpl) SampleSpans(serviceName string,
	fingerprint string,
	rl ptrace.ResourceSpans,
	sl ptrace.ScopeSpans,
	lr ptrace.Span) (droppingRule string) {
	ls.RLock()
	defer ls.RUnlock()

	return ls.shouldFilterSpan(serviceName, fingerprint, rl, sl, lr)
}

func (ls *EventSamplerImpl) shouldFilterSpan(serviceName string,
	fingerprint string,
	rl ptrace.ResourceSpans,
	sl ptrace.ScopeSpans,
	ll ptrace.Span) (droppingRule string) {
	if len(ls.rules) == 0 {
		return ""
	}

	ret := ""
	matched := false
	key := fmt.Sprintf("%s:%s", serviceName, fingerprint)
	randval := rand.Float64()

	for rid, r := range ls.rules {
		// if we already have a match, don't bother checking any more random rules.
		if matched && r.ruleType == EventSamplingRuleTypeRandom {
			continue
		}
		if r.evaluateSpan(rl, sl, ll) {
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

func (ls *EventSamplerImpl) shouldFilterLog(serviceName string,
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
		if matched && r.ruleType == EventSamplingRuleTypeRandom {
			continue
		}
		if r.evaluateLog(rl, sl, ll) {
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

func (ls *EventSamplerImpl) configure(config []EventSamplingConfigV1, vendor string) {
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
func (ls *EventSamplerImpl) addRule(c EventSamplingConfigV1) {
	ls.logger.Info("Adding event sampling rule", zap.String("id", c.Id), zap.Any("config", c))
	r := newFilterRule(c)

	r.ruleType, r.sampler = samplerForType(c, ls.logger)
	if r.sampler == nil {
		ls.logger.Error("Unknown event sampling rule type", zap.String("type", c.RuleType))
		return
	}
	if err := r.sampler.Start(); err != nil {
		ls.logger.Error("Error starting event sampler", zap.Error(err))
		return
	}
	ls.logger.Info("Started event sampling rule", zap.String("id", c.Id))
	ls.rules[c.Id] = r
}

func samplerForType(c EventSamplingConfigV1, logger *zap.Logger) (ruleType EventSamplingRuleType, sampler Sampler) {
	switch c.RuleType {
	case "random":
		return EventSamplingRuleTypeRandom, NewStaticSampler(int(1 / c.SampleRate))
	case "rps":
		switch c.RPS {
		case 0, 1:
			return EventSamplingRuleTypeRPS, NewStaticSampler(c.RPS)
		default:
			return EventSamplingRuleTypeRPS, NewRPSSampler(WithMaxRPS(c.RPS), WithLogger(logger))
		}
	}
	return EventSamplingRuleTypeUnknown, nil
}

// existing rule must be stopped and started by the caller.
func (ls *EventSamplerImpl) updateCurrentRule(r *filterRule, c EventSamplingConfigV1) {
	if r.config.Equals(c) {
		return
	}
	ls.logger.Info("Updating event sampling rule", zap.String("id", c.Id), zap.Any("config", c))
	_ = r.sampler.Stop()
	r.ruleType, r.sampler = samplerForType(c, ls.logger)
	if r.sampler == nil {
		ls.logger.Error("Unknown event sampling rule type", zap.String("type", c.RuleType))
		return
	}
	r.config = c
	r.parseConditions()

	if err := r.sampler.Start(); err != nil {
		ls.logger.Error("Error starting event sampler", zap.Error(err))
	}
	ls.logger.Info("Started log sampling rule", zap.String("id", c.Id))
}
