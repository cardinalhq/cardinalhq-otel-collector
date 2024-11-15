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

//
// NOTE!
// These must use YAML tags.  JSON tags are optional, but will be used if rendered/parsed as JSON.
// The config manager parses this as YAML.
//

type ControlPlaneConfig struct {
	// Processor targets
	Pitbulls       map[string]PitbullProcessorConfig        `json:"pitbulls,omitempty" yaml:"pitbulls,omitempty"`
	Stats          map[string]StatsProcessorConfig          `json:"stats,omitempty" yaml:"stats,omitempty"`
	ExtractMetrics map[string]ExtractMetricsProcessorConfig `json:"extract_metrics,omitempty" yaml:"extract_metrics,omitempty"`

	hash uint64
}

type PitbullProcessorConfig struct {
	LogStatements       []ContextStatement `json:"log_statements,omitempty" yaml:"log_statements,omitempty"`
	LogLookupConfigs    []LookupConfig     `json:"log_lookup_configs,omitempty" yaml:"log_lookup_configs,omitempty"`
	MetricStatements    []ContextStatement `json:"metric_statements,omitempty" yaml:"metric_statements,omitempty"`
	MetricLookupConfigs []LookupConfig     `json:"metric_lookup_configs,omitempty" yaml:"metric_lookup_configs,omitempty"`
	SpanStatements      []ContextStatement `json:"span_statements,omitempty" yaml:"span_statements,omitempty"`
	SpanLookupConfigs   []LookupConfig     `json:"span_lookup_configs,omitempty" yaml:"span_lookup_configs,omitempty"`
}

type StatsProcessorConfig struct {
	LogEnrichments    []StatsEnrichment `json:"log_enrichments,omitempty" yaml:"log_enrichments,omitempty"`
	MetricEnrichments []StatsEnrichment `json:"metric_enrichments,omitempty" yaml:"metric_enrichments,omitempty"`
	SpanEnrichments   []StatsEnrichment `json:"span_enrichments,omitempty" yaml:"span_enrichments,omitempty"`
}

type ExtractMetricsProcessorConfig struct {
	LogMetricExtractors  []MetricExtractorConfig `json:"log_metric_extractors,omitempty" yaml:"log_metric_extractors,omitempty"`
	SpanMetricExtractors []MetricExtractorConfig `json:"span_metric_extractors,omitempty" yaml:"span_metric_extractors,omitempty"`
}

type MetricExtractorConfig struct {
	RuleId      string            `json:"rule_id,omitempty" yaml:"rule_id,omitempty"`
	Conditions  []string          `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	Dimensions  map[string]string `json:"dimensions,omitempty" yaml:"dimensions,omitempty"`
	MetricName  string            `json:"metric_name,omitempty" yaml:"metric_name,omitempty"`
	MetricUnit  string            `json:"metric_unit,omitempty" yaml:"metric_unit,omitempty"`
	MetricType  string            `json:"metric_type,omitempty" yaml:"metric_type,omitempty"`
	MetricValue string            `json:"metric_value,omitempty" yaml:"metric_value,omitempty"`
}

type StatsEnrichment struct {
	Context string   `json:"context,omitempty" yaml:"context,omitempty"`
	Tags    []string `json:"tags,omitempty" yaml:"tags,omitempty"`
}

type SamplingConfig struct {
	SampleRate float64 `json:"sample_rate,omitempty" yaml:"sample_rate,omitempty"`
	RPS        int     `json:"rps,omitempty" yaml:"rps,omitempty"`
}

type Instruction struct {
	Statements []ContextStatement `json:"statements,omitempty" yaml:"statements,omitempty"`
}

type ContextID string

type ContextStatement struct {
	Context        ContextID      `json:"context,omitempty" yaml:"context,omitempty"`
	RuleId         RuleID         `json:"rule_id,omitempty" yaml:"rule_id,omitempty"`
	Priority       int            `json:"priority,omitempty" yaml:"priority,omitempty"`
	Conditions     []string       `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	Statements     []string       `json:"statements,omitempty" yaml:"statements,omitempty"`
	SamplingConfig SamplingConfig `json:"sampling_config,omitempty" yaml:"sampling_config,omitempty"`
}
