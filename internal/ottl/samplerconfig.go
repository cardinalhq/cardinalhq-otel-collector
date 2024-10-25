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

type ControlPlaneConfig struct {
	// Processor targets
	Pitbulls       map[string]PitbullProcessorConfig        `json:"pitbulls,omitempty"`
	Stats          map[string]StatsProcessorConfig          `json:"stats,omitempty"`
	ExtractMetrics map[string]ExtractMetricsProcessorConfig `json:"extract_metrics,omitempty"`

	hash uint64
}

type PitbullProcessorConfig struct {
	LogStatements       []ContextStatement `json:"log_statements,omitempty"`
	LogLookupConfigs    []LookupConfig     `json:"log_lookup_configs,omitempty"`
	MetricStatements    []ContextStatement `json:"metric_statements,omitempty"`
	MetricLookupConfigs []LookupConfig     `json:"metric_lookup_configs,omitempty"`
	SpanStatements      []ContextStatement `json:"span_statements,omitempty"`
	SpanLookupConfigs   []LookupConfig     `json:"span_lookup_configs,omitempty"`
}

type StatsProcessorConfig struct {
	LogEnrichments    []StatsEnrichment `json:"log_enrichments,omitempty"`
	MetricEnrichments []StatsEnrichment `json:"metric_enrichments,omitempty"`
	TraceEnrichments  []StatsEnrichment `json:"trace_enrichments,omitempty"`
}

type ExtractMetricsProcessorConfig struct {
	LogMetricExtractors  []MetricExtractorConfig `json:"log_metric_extractors,omitempty"`
	SpanMetricExtractors []MetricExtractorConfig `json:"span_metric_extractors,omitempty"`
}

type StatsEnrichment struct {
	Context string   `json:"context,omitempty"`
	Tags    []string `json:"tags,omitempty"`
}

type SamplingConfig struct {
	SampleRate float64 `json:"sample_rate,omitempty"`
	RPS        int     `json:"rps,omitempty"`
}

type Instruction struct {
	Statements []ContextStatement `json:"statements,omitempty"`
}

type ContextID string

type ContextStatement struct {
	Context        ContextID      `json:"context,omitempty"`
	RuleId         RuleID         `json:"rule_id,omitempty"`
	Priority       int            `json:"priority,omitempty"`
	Conditions     []string       `json:"conditions,omitempty"`
	Statements     []string       `json:"statements,omitempty"`
	SamplingConfig SamplingConfig `json:"sampling_config,omitempty"`
}
