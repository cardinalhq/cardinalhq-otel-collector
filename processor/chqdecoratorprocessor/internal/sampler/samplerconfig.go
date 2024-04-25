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
	Logs    LogConfig
	Metrics MetricConfig
}

type LogConfig struct {
	Sampling     []LogSamplingConfig
	Destinations []DestinationConfig
}

type LogSamplingConfig struct {
	Id         string
	RuleType   string
	Scope      map[string]string
	SampleRate float64
	RPS        int
}

type MetricConfig struct {
	Aggregators  []AggregatorConfig
	Destinations []DestinationConfig
}

type AggregatorConfig struct {
	Id         string
	Scope      map[string]string
	MetricName string
	Tags       []string
	TagAction  string
}

type DestinationConfig struct {
	Provider string
	Host     string
	APIKey   string
}
