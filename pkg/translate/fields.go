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

package translate

const (
	CardinalFieldPrefix    = "_cardinalhq"
	CardinalFieldPrefixDot = CardinalFieldPrefix + "."

	CardinalFieldBucketBounds      = CardinalFieldPrefixDot + "bucket_bounds"
	CardinalFieldCounts            = CardinalFieldPrefixDot + "counts"
	CardinalFieldFiltered          = CardinalFieldPrefixDot + "filtered"
	CardinalFieldFingerprint       = CardinalFieldPrefixDot + "fingerprint"
	CardinalFieldDropForVendor     = CardinalFieldPrefixDot + "drop_for_vendor"
	CardinalFieldRulesMatched      = CardinalFieldPrefixDot + "rules_matched"
	CardinalFieldHostname          = CardinalFieldPrefixDot + "hostname"
	CardinalFieldID                = CardinalFieldPrefixDot + "id"
	CardinalFieldMessage           = CardinalFieldPrefixDot + "message"
	CardinalFieldMetricType        = CardinalFieldPrefixDot + "metric_type"
	CardinalFieldName              = CardinalFieldPrefixDot + "name"
	CardinalFieldNegativeCounts    = CardinalFieldPrefixDot + "negative_counts"
	CardinalFieldPositiveCounts    = CardinalFieldPrefixDot + "positive_counts"
	CardinalFieldResourceSchemaURL = CardinalFieldPrefixDot + "resource_schemaurl"
	CardinalFieldRuleConfig        = CardinalFieldPrefixDot + "ruleconfig"
	CardinalFieldRuleID            = CardinalFieldPrefixDot + "rule_id"
	CardinalFieldRuleMatch         = CardinalFieldPrefixDot + "rule_match"
	CardinalFieldLevel             = CardinalFieldPrefixDot + "level"
	CardinalFieldScale             = CardinalFieldPrefixDot + "scale"
	CardinalFieldSpanDuration      = CardinalFieldPrefixDot + "span_duration"
	CardinalFieldSpanIsSlow        = CardinalFieldPrefixDot + "isSlow"
	CardinalFieldSpanEndTime       = CardinalFieldPrefixDot + "span_end_time"
	CardinalFieldSpanEventcount    = CardinalFieldPrefixDot + "span_eventcount"
	CardinalFieldSpanKind          = CardinalFieldPrefixDot + "span_kind"
	CardinalFieldSpanParentSpanID  = CardinalFieldPrefixDot + "span_parent_span_id"
	CardinalFieldSpanSpanID        = CardinalFieldPrefixDot + "span_span_id"
	CardinalFieldSpanStartTime     = CardinalFieldPrefixDot + "span_start_time"
	CardinalFieldSpanStatusCode    = CardinalFieldPrefixDot + "span_status_code"
	CardinalFieldSpanStatusMessage = CardinalFieldPrefixDot + "span_status_message"
	CardinalFieldSpanTraceID       = CardinalFieldPrefixDot + "span_trace_id"
	CardinalFieldTelemetryType     = CardinalFieldPrefixDot + "telemetry_type"
	CardinalFieldTID               = CardinalFieldPrefixDot + "tid"
	CardinalFieldTimestamp         = CardinalFieldPrefixDot + "timestamp"
	CardinalFieldValue             = CardinalFieldPrefixDot + "value"
	CardinalFieldZeroCount         = CardinalFieldPrefixDot + "zero_count"
	CardinalFieldSpanName          = CardinalFieldPrefixDot + "span_name"
	CardinalFieldResourceName      = CardinalFieldPrefixDot + "resource_name"
	CardinalFieldScopeSchemaURL    = CardinalFieldPrefixDot + "scope_schemaurl"
	CardinalFieldAggregate         = CardinalFieldPrefixDot + "aggregate"
	CardinalFieldWouldFilter       = CardinalFieldPrefixDot + "would_filter"
	CardinalFieldFilteredReason    = CardinalFieldPrefixDot + "filtered_reason"
	CardinalFieldTraceHasError     = CardinalFieldPrefixDot + "trace_has_error"
	CardinalFieldSpanHasError      = CardinalFieldPrefixDot + "span_has_error"
	CardinalFieldIsRootSpan        = CardinalFieldPrefixDot + "is_root_span"
	CardinalFieldClassification    = CardinalFieldPrefixDot + "classification"
	CardinalFieldFingerprintError  = CardinalFieldPrefixDot + "fingerprint_error"
	CardinalFieldDecoratorPodName  = CardinalFieldPrefixDot + "decorator.pod_name"
	CardinalFieldCustomerID        = CardinalFieldPrefixDot + "customer_id"
	CardinalFieldCustomerName      = CardinalFieldPrefixDot + "customer_name"
	CardinalFieldCollectorID       = CardinalFieldPrefixDot + "collector_id"
	CardinalFieldDropMarker        = CardinalFieldPrefixDot + "drop_marker"
	CardinalFieldReceiverType      = CardinalFieldPrefixDot + "receiver_type"
	ConditionsMatched              = "conditions_matched"
	StatementsEvaluated            = "statements_evaluated"
	SamplerAllowed                 = "sampler_allowed"
	RuleId                         = "rule_id"
	Stage                          = "stage"
	ErrorMsg                       = "errorMsg"

	CardinalMetricTypeCount                = "count"
	CardinalMetricTypeExponentialHistogram = "exponential_histogram"
	CardinalMetricTypeGauge                = "gauge"
	CardinalMetricTypeHistogram            = "histogram"

	CardinalTelemetryTypeLogs    = "logs"
	CardinalTelemetryTypeMetrics = "metrics"
	CardinalTelemetryTypeTraces  = "traces"

	CardinalHeaderAPIKey   = "X-CardinalHQ-API-Key"
	CardinalHeaderDDAPIKey = "DD-API-KEY"
)
