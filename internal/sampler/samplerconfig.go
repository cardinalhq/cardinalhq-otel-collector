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
	Decorators []ottl.Instruction `json:"decorators,omitempty" yaml:"decorators,omitempty"`
	Enforcers  []ottl.Instruction `json:"enforcers,omitempty" yaml:"enforcers,omitempty"`
}

type MetricConfigV1 struct {
	Decorators []ottl.Instruction `json:"decorators,omitempty" yaml:"decorators,omitempty"`
	Enforcers  []ottl.Instruction `json:"enforcers,omitempty" yaml:"enforcers,omitempty"`
}

type Filter struct {
	ContextId string `json:"contextId,omitempty" yaml:"contextId,omitempty"`
	Condition string `json:"condition,omitempty" yaml:"condition,omitempty"`
}

func (f Filter) Equals(other Filter) bool {
	return f.ContextId == other.ContextId && f.Condition == other.Condition
}
