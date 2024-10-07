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
	Logs    LogConfigV1    `json:"logs,omitempty" yaml:"logs,omitempty"`
	Metrics MetricConfigV1 `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	Traces  TraceConfigV1  `json:"traces,omitempty" yaml:"traces,omitempty"`

	hash uint64
}

type LogConfigV1 struct {
	Transformations []ottl.ContextStatement `json:"transformations,omitempty" yaml:"transformations,omitempty"`
	Sampling        []EventSamplingConfigV1 `json:"sampling,omitempty" yaml:"sampling,omitempty"`
}

type TraceConfigV1 struct {
	Transformations []ottl.ContextStatement `json:"transformations,omitempty" yaml:"transformations,omitempty"`
	Sampling        []EventSamplingConfigV1 `json:"sampling,omitempty" yaml:"sampling,omitempty"`
}

type Filter struct {
	ContextId string `json:"contextId,omitempty" yaml:"contextId,omitempty"`
	Condition string `json:"condition,omitempty" yaml:"condition,omitempty"`
}

type EventSamplingConfigV1 struct {
	Id         string   `json:"id,omitempty" yaml:"id,omitempty"`
	RuleType   string   `json:"ruleType,omitempty" yaml:"ruleType,omitempty"`
	Filter     []Filter `json:"filter,omitempty" yaml:"filter,omitempty"`
	SampleRate float64  `json:"sampleRate,omitempty" yaml:"sampleRate,omitempty"`
	RPS        int      `json:"rps,omitempty" yaml:"rps,omitempty"`
	Vendor     string   `json:"vendor,omitempty" yaml:"vendor,omitempty"`
}

type MetricConfigV1 struct {
	// General transformations that apply to all metrics for e.g. team associations.
	Transformations []ottl.ContextStatement `json:"transformations,omitempty" yaml:"transformations,omitempty"`
	Aggregators     []AggregatorConfigV1    `json:"aggregators,omitempty" yaml:"aggregators,omitempty"`
}

type AggregatorConfigV1 struct {
	Id              string                  `json:"id,omitempty" yaml:"id,omitempty"`
	MetricName      string                  `json:"metricName,omitempty" yaml:"metricName,omitempty"`
	Transformations []ottl.ContextStatement `json:"transformations,omitempty" yaml:"transformations,omitempty"`
	Vendor          string                  `json:"vendor,omitempty" yaml:"vendor,omitempty"`
}

func (lsc EventSamplingConfigV1) Equals(other EventSamplingConfigV1) bool {
	if lsc.Id != other.Id ||
		lsc.RPS != other.RPS ||
		lsc.RuleType != other.RuleType ||
		lsc.SampleRate != other.SampleRate ||
		lsc.RPS != other.RPS &&
			lsc.Vendor != other.Vendor {
		return false
	}

	if len(lsc.Filter) != len(other.Filter) {
		return false
	}

	for i := range lsc.Filter {
		if lsc.Filter[i].ContextId != other.Filter[i].ContextId ||
			lsc.Filter[i].Condition != other.Filter[i].Condition {
			return false
		}
	}
	return true
}
