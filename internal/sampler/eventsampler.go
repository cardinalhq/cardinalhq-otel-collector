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
	"math/rand"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"go.uber.org/zap"
)

type EventSampler interface {
	SampleLogs(serviceName string, fingerprint string, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) []SamplingRuleMatch
	SampleSpans(serviceName string, fingerprint string, rl ptrace.ResourceSpans, sl ptrace.ScopeSpans, lr ptrace.Span) []SamplingRuleMatch
	UpdateConfig(config []SamplingRule, telemetry component.TelemetrySettings)
}

type SamplingRuleMatch struct {
	RuleId   string
	VendorId string
}

var _ EventSampler = (*EventSamplerImpl)(nil)

type EventSamplerImpl struct {
	sync.RWMutex
	rules map[string]map[string]*filterRule

	logger *zap.Logger
}

func NewEventSamplerImpl(ctx context.Context, logger *zap.Logger) *EventSamplerImpl {
	ls := &EventSamplerImpl{
		logger: logger,
		rules:  map[string]map[string]*filterRule{},
	}
	return ls
}

func (ls *EventSamplerImpl) UpdateConfig(config []SamplingRule, telemetry component.TelemetrySettings) {
	ls.Lock()
	defer ls.Unlock()
	ls.configure(config, telemetry)
}

func (ls *EventSamplerImpl) SampleLogs(serviceName string,
	fingerprint string,
	rl plog.ResourceLogs,
	sl plog.ScopeLogs,
	ll plog.LogRecord) []SamplingRuleMatch {
	ls.RLock()
	defer ls.RUnlock()

	return ls.shouldFilterLog(serviceName, fingerprint, rl, sl, ll)
}

func (ls *EventSamplerImpl) SampleSpans(serviceName string,
	fingerprint string,
	rl ptrace.ResourceSpans,
	sl ptrace.ScopeSpans,
	lr ptrace.Span) []SamplingRuleMatch {
	ls.RLock()
	defer ls.RUnlock()
	return ls.shouldFilterSpan(serviceName, fingerprint, rl, sl, lr)
}

func (ls *EventSamplerImpl) shouldFilterSpan(serviceName string,
	fingerprint string,
	rl ptrace.ResourceSpans,
	sl ptrace.ScopeSpans,
	ll ptrace.Span) []SamplingRuleMatch {
	if len(ls.rules) == 0 {
		return []SamplingRuleMatch{}
	}

	var ret []SamplingRuleMatch
	matchedByVendorId := make(map[string]bool)
	key := fmt.Sprintf("%s:%s", serviceName, fingerprint)
	randval := rand.Float64()

	for vendorId, rules := range ls.rules {
		for rid, r := range rules {
			if matchedByVendorId[vendorId] && r.ruleType == EventSamplingRuleTypeRandom {
				continue
			}
			if r.evaluateSpan(rl, sl, ll) {
				rate := r.sampler.GetSampleRate(key)
				wasHit := shouldFilter(rate, randval)
				if wasHit {
					ret = append(ret, SamplingRuleMatch{RuleId: rid, VendorId: vendorId})
					matchedByVendorId[vendorId] = true
				}
			}
		}
	}

	return ret
}

func (ls *EventSamplerImpl) shouldFilterLog(serviceName string,
	fingerprint string,
	rl plog.ResourceLogs,
	sl plog.ScopeLogs,
	ll plog.LogRecord) []SamplingRuleMatch {
	if len(ls.rules) == 0 {
		return []SamplingRuleMatch{}
	}

	var ret []SamplingRuleMatch

	matchedByVendorId := make(map[string]bool)
	key := fmt.Sprintf("%s:%s", serviceName, fingerprint)
	randval := rand.Float64()

	for vendorId, rules := range ls.rules {
		for rid, r := range rules {
			if matchedByVendorId[vendorId] && r.ruleType == EventSamplingRuleTypeRandom {
				continue
			}
			if r.evaluateLog(rl, sl, ll) {
				rate := r.sampler.GetSampleRate(key)
				wasHit := shouldFilter(rate, randval)
				if wasHit {
					ret = append(ret, SamplingRuleMatch{RuleId: rid, VendorId: vendorId})
					matchedByVendorId[vendorId] = true
				}
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

func (ls *EventSamplerImpl) configure(config []SamplingRule, telemetry component.TelemetrySettings) {
	ls.logger.Info("Updating event sampling rules", zap.Any("config", config))

	// Track current rules by vendor and rule ID
	currentIDs := map[string]map[string]bool{}
	for vendorId, vendorRules := range ls.rules {
		currentIDs[vendorId] = map[string]bool{}
		for ruleId := range vendorRules {
			currentIDs[vendorId][ruleId] = true
		}
	}

	// Process the new configuration
	for _, c := range config {
		vendorId := c.VendorId
		if vendorId == "" {
			ls.logger.Error("Vendor ID is missing for rule", zap.String("ruleId", c.RuleId))
			continue
		}

		// Ensure the rule type is valid
		if c.RuleType != "random" && c.RuleType != "rps" {
			ls.logger.Error("Unknown event sampling rule type", zap.String("type", c.RuleType), zap.String("ruleId", c.RuleId))
			continue
		}

		// Initialize vendor's rule map if it doesn't exist
		if ls.rules[vendorId] == nil {
			ls.rules[vendorId] = make(map[string]*filterRule)
		}

		// Check if rule already exists and update or add it
		if currentRule, ok := ls.rules[vendorId][c.RuleId]; ok {
			if err := ls.updateCurrentRule(currentRule, c, telemetry); err != nil {
				ls.logger.Error("Error updating event sampling rule (removing)", zap.String("ruleId", c.RuleId), zap.Error(err))
				continue
			}
		} else {
			if err := ls.addRule(c, telemetry); err != nil {
				ls.logger.Error("Error adding event sampling rule", zap.String("ruleId", c.RuleId), zap.Error(err))
			}
		}

		// Remove this rule from the currentIDs map (since it's up to date now)
		delete(currentIDs[vendorId], c.RuleId)
		if len(currentIDs[vendorId]) == 0 {
			delete(currentIDs, vendorId) // Clean up empty vendor entries
		}
	}

	// Clean up any old rules that weren't in the new configuration
	for vendorId, vendorRules := range currentIDs {
		for ruleId := range vendorRules {
			r := ls.rules[vendorId][ruleId]
			_ = r.sampler.Stop()
			ls.logger.Info("Removing event sampling rule", zap.String("vendorId", vendorId), zap.String("ruleId", ruleId))
			delete(ls.rules[vendorId], ruleId)
		}

		// Clean up the vendor entry if no more rules exist for this vendor
		if len(ls.rules[vendorId]) == 0 {
			delete(ls.rules, vendorId)
		}
	}
}

// new rule must be started by the caller.
func (ls *EventSamplerImpl) addRule(c SamplingRule, telemetry component.TelemetrySettings) error {
	ls.logger.Info("Adding event sampling rule", zap.String("ruleId", c.RuleId), zap.Any("config", c))

	// Create a new filter rule based on the sampling rule configuration
	r, err := newFilterRule(c, telemetry)
	if err != nil {
		return fmt.Errorf("error creating event sampling rule: %w", err)
	}

	// Determine the sampler based on the rule type
	r.ruleType, r.sampler = samplerForType(c, ls.logger)
	if r.sampler == nil {
		return fmt.Errorf("unknown event sampling rule type %s", c.RuleType)
	}

	// Start the sampler
	if err := r.sampler.Start(); err != nil {
		return fmt.Errorf("error starting event sampler: %w", err)
	}

	// Log the rule addition
	ls.logger.Info("Started event sampling rule", zap.String("ruleId", c.RuleId))

	// Initialize the map for the vendor if it doesn't exist
	if ls.rules[c.VendorId] == nil {
		ls.rules[c.VendorId] = make(map[string]*filterRule)
	}

	// Add the rule to the map under the appropriate vendorId and ruleId
	ls.rules[c.VendorId][c.RuleId] = r

	return nil
}

func samplerForType(c SamplingRule, logger *zap.Logger) (ruleType EventSamplingRuleType, sampler Sampler) {
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
func (ls *EventSamplerImpl) updateCurrentRule(r *filterRule, c SamplingRule, telemetry component.TelemetrySettings) error {
	if r.config.Equals(c) {
		return nil
	}
	ls.logger.Info("Updating event sampling rule", zap.String("id", c.RuleId), zap.Any("config", c))
	_ = r.sampler.Stop()
	r.ruleType, r.sampler = samplerForType(c, ls.logger)
	if r.sampler == nil {
		return fmt.Errorf("unknown event sampling rule type %s", c.RuleType)
	}
	r.config = c
	if err := r.parseConditions(telemetry); err != nil {
		return fmt.Errorf("error parsing conditions: %w", err)
	}

	if err := r.sampler.Start(); err != nil {
		return fmt.Errorf("error starting event sampler: %w", err)
	}
	ls.logger.Info("Started log sampling rule", zap.String("id", c.RuleId))
	return nil
}
