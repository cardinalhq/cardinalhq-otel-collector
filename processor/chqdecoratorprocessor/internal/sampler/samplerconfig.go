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

type SamplerConfig struct {
	Logs    LogConfig    `json:"logs,omitempty" yaml:"logs,omitempty"`
	Metrics MetricConfig `json:"metrics,omitempty" yaml:"metrics,omitempty"`

	hash uint64
}

type LogConfig struct {
	Sampling     []LogSamplingConfig `json:"sampling,omitempty" yaml:"sampling,omitempty"`
	Destinations []DestinationConfig `json:"destinations,omitempty" yaml:"destinations,omitempty"`
}

type LogSamplingConfig struct {
	Id         string            `json:"id,omitempty" yaml:"id,omitempty"`
	RuleType   string            `json:"ruleType,omitempty" yaml:"ruleType,omitempty"`
	Scope      map[string]string `json:"scope,omitempty" yaml:"scope,omitempty"`
	SampleRate float64           `json:"sampleRate,omitempty" yaml:"sampleRate,omitempty"`
	RPS        int               `json:"rps,omitempty" yaml:"rps,omitempty"`
}

type MetricConfig struct {
	Aggregators  []AggregatorConfig  `json:"aggregators,omitempty" yaml:"aggregators,omitempty"`
	Destinations []DestinationConfig `json:"destinations,omitempty" yaml:"destinations,omitempty"`
}

type AggregatorConfig struct {
	Id         string            `json:"id,omitempty" yaml:"id,omitempty"`
	Scope      map[string]string `json:"scope,omitempty" yaml:"scope,omitempty"`
	MetricName string            `json:"metricName,omitempty" yaml:"metricName,omitempty"`
	Tags       []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
	TagAction  string            `json:"tagAction,omitempty" yaml:"tagAction,omitempty"`
}

type DestinationConfig struct {
	Provider string `json:"provider,omitempty" yaml:"provider,omitempty"`
	Host     string `json:"host,omitempty" yaml:"host,omitempty"`
	APIKey   string `json:"apiKey,omitempty" yaml:"apiKey,omitempty"`
}
