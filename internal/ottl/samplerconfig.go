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

package ottl

type SamplerConfig struct {
	Logs    EventConfigV1  `json:"logs,omitempty" yaml:"logs,omitempty"`
	Metrics MetricConfigV1 `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	Spans   EventConfigV1  `json:"spans,omitempty" yaml:"spans,omitempty"`

	hash uint64
}

type EventConfigV1 struct {
	Decorators []Instruction `json:"decorators,omitempty" yaml:"decorators,omitempty"`
	Enforcers  []Instruction `json:"enforcers,omitempty" yaml:"enforcers,omitempty"`
}

type MetricConfigV1 struct {
	Decorators []Instruction `json:"decorators,omitempty" yaml:"decorators,omitempty"`
	Enforcers  []Instruction `json:"enforcers,omitempty" yaml:"enforcers,omitempty"`
}

type ContextID string

type SamplingConfig struct {
	RuleType   int     `json:"ruleType,omitempty" yaml:"ruleType,omitempty"`
	SampleRate float64 `json:"sampleRate,omitempty" yaml:"sampleRate,omitempty"`
	RPS        int     `json:"rps,omitempty" yaml:"rps,omitempty"`
}

type Instruction struct {
	VendorId   VendorID           `json:"vendorId,omitempty" yaml:"vendorId,omitempty"`
	Statements []ContextStatement `json:"statements,omitempty" yaml:"statements,omitempty"`
}

type ContextStatement struct {
	Context        ContextID      `mapstructure:"context"`
	RuleId         RuleID         `mapstructure:"ruleId"`
	Priority       int            `mapstructure:"priority"`
	Conditions     []string       `mapstructure:"conditions"`
	Statements     []string       `mapstructure:"statements"`
	SamplingConfig SamplingConfig `mapstructure:"samplingConfig"`
}
