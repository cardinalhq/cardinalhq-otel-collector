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

import "github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"

type SamplerConfig struct {
	Logs    EventConfigV1  `json:"logs,omitempty" yaml:"logs,omitempty"`
	Metrics MetricConfigV1 `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	Traces  EventConfigV1  `json:"traces,omitempty" yaml:"traces,omitempty"`

	hash uint64
}

type EventConfigV1 struct {
	Decorators    []Instruction  `json:"decorators,omitempty" yaml:"decorators,omitempty"`
	Enforcers     []Instruction  `json:"enforcers,omitempty" yaml:"enforcers,omitempty"`
	SamplingRules []SamplingRule `json:"samplingRules,omitempty" yaml:"samplingRules,omitempty"`
}

type MetricConfigV1 struct {
	Decorators []Instruction `json:"decorators,omitempty" yaml:"decorators,omitempty"`
	Enforcers  []Instruction `json:"enforcers,omitempty" yaml:"enforcers,omitempty"`
}

type Instruction struct {
	VendorId   string                  `json:"vendorId,omitempty" yaml:"vendorId,omitempty"`
	Statements []ottl.ContextStatement `json:"statements,omitempty" yaml:"statements,omitempty"`
}

type Filter struct {
	ContextId string `json:"contextId,omitempty" yaml:"contextId,omitempty"`
	Condition string `json:"condition,omitempty" yaml:"condition,omitempty"`
}

func (f Filter) Equals(other Filter) bool {
	return f.ContextId == other.ContextId && f.Condition == other.Condition
}

type SamplingRule struct {
	RuleId     string   `json:"ruleId,omitempty" yaml:"ruleId,omitempty"`
	Priority   int      `json:"priority,omitempty" yaml:"priority,omitempty"`
	VendorId   string   `json:"vendorId,omitempty" yaml:"vendorId,omitempty"`
	Conditions []Filter `json:"filter,omitempty" yaml:"filter,omitempty"`
	SampleRate float64  `json:"sampleRate,omitempty" yaml:"sampleRate,omitempty"`
	RPS        int      `json:"rps,omitempty" yaml:"rps,omitempty"`
	RuleType   string   `json:"ruleType,omitempty" yaml:"ruleType,omitempty"`
}

func (sr SamplingRule) Equals(other SamplingRule) bool {
	// Compare simple fields
	if sr.RuleId != other.RuleId ||
		sr.Priority != other.Priority ||
		sr.VendorId != other.VendorId ||
		sr.SampleRate != other.SampleRate ||
		sr.RPS != other.RPS ||
		sr.RuleType != other.RuleType {
		return false
	}

	// Compare Conditions slices length
	if len(sr.Conditions) != len(other.Conditions) {
		return false
	}

	// Compare Conditions slice content
	for i, condition := range sr.Conditions {
		if !condition.Equals(other.Conditions[i]) {
			return false
		}
	}

	// All fields are equal
	return true
}
