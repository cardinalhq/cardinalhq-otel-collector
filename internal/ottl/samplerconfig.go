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

type PitbullProcessorConfig struct {
	LogStatements    []ContextStatement `json:"logs_statements,omitempty"`
	MetricStatements []ContextStatement `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	SpanStatements   []ContextStatement `json:"spans,omitempty" yaml:"spans,omitempty"`

	LogsLookupConfigs    []LookupConfig `json:"logs_lookup_configs,omitempty" yaml:"logs_lookup_configs,omitempty"`
	TracesLookupConfigs  []LookupConfig `json:"spans_lookup_configs,omitempty" yaml:"spans_lookup_configs,omitempty"`
	MetricsLookupConfigs []LookupConfig `json:"metrics_lookup_configs,omitempty" yaml:"metrics_lookup_configs,omitempty"`
}

type StatsProcessorConfig struct {
	LogsEnrichments    []StatsEnrichment `json:"logs_enrichments,omitempty" mapstructure:"logs_enrichments"`
	MetricsEnrichments []StatsEnrichment `json:"metrics_enrichments,omitempty" mapstructure:"metrics_enrichments"`
	TracesEnrichments  []StatsEnrichment `json:"traces_enrichments,omitempty" mapstructure:"traces_enrichments"`
}

type ExtractMetricsProcessorConfig struct {
	LogMetricExtractors  []MetricExtractorConfig `json:"log_extractors"`
	SpanMetricExtractors []MetricExtractorConfig `json:"span_extractors"`
}

type ControlPlaneConfig struct {
	Pitbulls       map[string]PitbullProcessorConfig        `json:"pitbulls,omitempty" yaml:"pitbulls,omitempty"`
	Stats          map[string]StatsProcessorConfig          `json:"stats,omitempty" yaml:"stats,omitempty"`
	ExtractMetrics map[string]ExtractMetricsProcessorConfig `json:"extract_metrics,omitempty" yaml:"extract_metrics,omitempty"`

	hash uint64
}

type StatsEnrichment struct {
	Context string   `json:"context,omitempty" mapstructure:"context"`
	Tags    []string `json:"tags,omitempty" mapstructure:"tags"`
}

type SamplingConfig struct {
	SampleRate float64 `json:"sampleRate,omitempty" yaml:"sampleRate,omitempty"`
	RPS        int     `json:"rps,omitempty" yaml:"rps,omitempty"`
}

type Instruction struct {
	Statements []ContextStatement `json:"statements,omitempty" yaml:"statements,omitempty"`
}

type ContextID string

type ContextStatement struct {
	Context        ContextID      `json:"context,omitempty" yaml:"context,omitempty"`
	RuleId         RuleID         `json:"ruleId,omitempty" yaml:"ruleId,omitempty"`
	Priority       int            `json:"priority,omitempty" yaml:"priority,omitempty"`
	Conditions     []string       `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	Statements     []string       `json:"statements,omitempty" yaml:"statements,omitempty"`
	SamplingConfig SamplingConfig `json:"samplingConfig,omitempty" yaml:"samplingConfig,omitempty"`
}
