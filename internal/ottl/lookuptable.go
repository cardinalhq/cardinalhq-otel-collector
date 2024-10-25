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

import (
	"context"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type LookupCondition struct {
	ColumnName     string `json:"tag_name"`
	OTTLExpression string `json:"expression"`
	ColumnValue    string `json:"tag_value"`

	ParsedLogExpression    *ottl.Statement[ottllog.TransformContext]
	ParsedSpanExpression   *ottl.Statement[ottlspan.TransformContext]
	ParsedMetricExpression *ottl.Statement[ottldatapoint.TransformContext]
}

type LookupRule struct {
	TagNameToSet string             `json:"tag_name"`
	Conditions   []*LookupCondition `json:"conditions"`
}

type LookupConfig struct {
	TableName string      `json:"table_name"`
	Table     LookupTable `json:"table"`

	LogRules     []LookupRule `json:"log_rules"`
	SpanRules    []LookupRule `json:"span_rules"`
	MetricsRules []LookupRule `json:"metrics_rules"`

	LogQualifiers    []string `json:"log_qualifiers"`
	SpanQualifiers   []string `json:"span_qualifiers"`
	MetricQualifiers []string `json:"metric_qualifiers"`

	ParsedLogQualifiers    []*ottl.Condition[ottllog.TransformContext]
	ParsedSpanQualifiers   []*ottl.Condition[ottlspan.TransformContext]
	ParsedMetricQualifiers []*ottl.Condition[ottldatapoint.TransformContext]
}

func (lc *LookupConfig) Init(logger *zap.Logger) {
	if len(lc.LogRules) > 0 || len(lc.LogQualifiers) > 0 {
		logParser, _ := ottllog.NewParser(ToFactory[ottllog.TransformContext](), component.TelemetrySettings{Logger: logger})
		qualifiers, logError := logParser.ParseConditions(lc.LogQualifiers)
		if logError != nil {
			logger.Error("Error parsing log qualifiers", zap.Error(logError))
			return
		}
		lc.ParsedLogQualifiers = qualifiers
		for _, logRule := range lc.LogRules {
			for _, condition := range logRule.Conditions {
				parsedLogExpression, err := logParser.ParseStatement(fmt.Sprintf("value(%s)", condition.OTTLExpression))
				if err != nil {
					logger.Error("Error parsing log expression", zap.Error(logError))
					return
				}
				condition.ParsedLogExpression = parsedLogExpression
			}
		}
	}

	// for spans
	if len(lc.SpanRules) > 0 || len(lc.SpanQualifiers) > 0 {
		spanParser, _ := ottlspan.NewParser(ToFactory[ottlspan.TransformContext](), component.TelemetrySettings{Logger: logger})
		qualifiers, logError := spanParser.ParseConditions(lc.LogQualifiers)
		if logError != nil {
			logger.Error("Error parsing span qualifiers", zap.Error(logError))
			return
		}
		lc.ParsedSpanQualifiers = qualifiers
		for _, logRule := range lc.LogRules {
			for _, condition := range logRule.Conditions {
				parsedSpanExpression, err := spanParser.ParseStatement(fmt.Sprintf("value(%s)", condition.OTTLExpression))
				if err != nil {
					logger.Error("Error parsing log expression", zap.Error(logError))
					return
				}
				condition.ParsedSpanExpression = parsedSpanExpression
			}
		}
	}

	// for metric data points
	if len(lc.MetricsRules) > 0 || len(lc.MetricQualifiers) > 0 {
		metricsParser, _ := ottldatapoint.NewParser(ToFactory[ottldatapoint.TransformContext](), component.TelemetrySettings{Logger: logger})
		qualifiers, logError := metricsParser.ParseConditions(lc.MetricQualifiers)
		if logError != nil {
			logger.Error("Error parsing span qualifiers", zap.Error(logError))
			return
		}
		lc.ParsedMetricQualifiers = qualifiers
		for _, logRule := range lc.LogRules {
			for _, condition := range logRule.Conditions {
				parsedMetricExpression, err := metricsParser.ParseStatement(fmt.Sprintf("value(%s)", condition.OTTLExpression))
				if err != nil {
					logger.Error("Error parsing log expression", zap.Error(logError))
					return
				}
				condition.ParsedMetricExpression = parsedMetricExpression
			}
		}
	}
}

// ExecuteLogsRule executes the log rules for the given record
func (lc *LookupConfig) ExecuteLogsRule(logger *zap.Logger, ctx context.Context, tCtx ottllog.TransformContext, record plog.LogRecord) {
	for _, lr := range lc.LogRules {
		tagToSet := lr.TagNameToSet
		conditionsArray := make([]string, 0, len(lr.Conditions)*2)

		for _, lookupCondition := range lr.Conditions {
			expression := lookupCondition.ParsedLogExpression
			if expression != nil {
				attrVal, _, err := expression.Execute(ctx, tCtx)
				if err != nil {
					return
				}
				if attrVal != nil {
					conditionsArray = append(conditionsArray, lookupCondition.ColumnName, attrVal.(string))
				}
			}
		}
		if len(lr.Conditions) > 0 && len(conditionsArray) == 0 {
			return
		}
		targetValue, found := lc.Table.Lookup(tagToSet, logger, conditionsArray)
		if found {
			record.Attributes().PutStr(tagToSet, targetValue)
		}
	}
}

// ExecuteSpansRule executes the span rules for the given record
func (lc *LookupConfig) ExecuteSpansRule(logger *zap.Logger, ctx context.Context, tCtx ottlspan.TransformContext, record ptrace.Span) {
	for _, lr := range lc.SpanRules {
		tagToSet := lr.TagNameToSet
		conditionsArray := make([]string, 0, len(lr.Conditions)*2)

		for _, lookupCondition := range lr.Conditions {
			expression := lookupCondition.ParsedSpanExpression
			if expression != nil {
				attrVal, _, err := expression.Execute(ctx, tCtx)
				if err != nil {
					return
				}
				if attrVal != nil {
					conditionsArray = append(conditionsArray, lookupCondition.ColumnName, attrVal.(string))
				}
			}
		}
		if len(lr.Conditions) > 0 && len(conditionsArray) == 0 {
			return
		}
		targetValue, found := lc.Table.Lookup(tagToSet, logger, conditionsArray)
		if found {
			record.Attributes().PutStr(tagToSet, targetValue)
		}
	}
}

// ExecuteMetricsRule executes the metrics rules for the given record
func (lc *LookupConfig) ExecuteMetricsRule(logger *zap.Logger, ctx context.Context, tCtx ottldatapoint.TransformContext, handlerFunc func(tagToSet string, targetValue string)) {
	for _, lr := range lc.MetricsRules {
		tagToSet := lr.TagNameToSet
		conditionsArray := make([]string, 0, len(lr.Conditions)*2)

		for _, lookupCondition := range lr.Conditions {
			expression := lookupCondition.ParsedMetricExpression
			if expression != nil {
				attrVal, _, err := expression.Execute(ctx, tCtx)
				if err != nil {
					return
				}
				if attrVal != nil {
					conditionsArray = append(conditionsArray, lookupCondition.ColumnName, attrVal.(string))
				}
			}
		}
		if len(lr.Conditions) > 0 && len(conditionsArray) == 0 {
			return
		}
		targetValue, found := lc.Table.Lookup(tagToSet, logger, conditionsArray)
		if found {
			handlerFunc(tagToSet, targetValue)
		}
	}
}

// ExecuteDataPointRule executes the data point rules for the given record
func (lc *LookupConfig) ExecuteDataPointRule(rules []LookupRule, logger *zap.Logger, ctx context.Context, tCtx ottlspan.TransformContext, record ptrace.Span) {
	for _, lr := range rules {
		tagToSet := lr.TagNameToSet
		conditionsArray := make([]string, 0, len(lr.Conditions)*2)

		for _, lookupCondition := range lr.Conditions {
			attrVal, _, err := lookupCondition.ParsedSpanExpression.Execute(ctx, tCtx)
			if err != nil {
				return
			}
			if attrVal != nil {
				conditionsArray = append(conditionsArray, lookupCondition.ColumnName, attrVal.(string))
			}
		}
		targetValue, found := lc.Table.Lookup(tagToSet, logger, conditionsArray)
		if found {
			record.Attributes().PutStr(tagToSet, targetValue)
		}
	}
}

// QualifiesForLogRecord checks if the log record qualifies for the lookup
func (lc *LookupConfig) QualifiesForLogRecord(ctx context.Context, tCtx ottllog.TransformContext) bool {
	if len(lc.LogQualifiers) == 0 {
		return true
	}
	for _, qualifier := range lc.ParsedLogQualifiers {
		matched, err := qualifier.Eval(ctx, tCtx)
		if err != nil {
			return false
		}
		if !matched {
			return false
		}
	}
	return true
}

// QualifiesForSpanRecord checks if the span record qualifies for the lookup
func (lc *LookupConfig) QualifiesForSpanRecord(ctx context.Context, tCtx ottlspan.TransformContext) bool {
	if len(lc.SpanQualifiers) == 0 {
		return true
	}
	for _, qualifier := range lc.ParsedSpanQualifiers {
		matched, err := qualifier.Eval(ctx, tCtx)
		if err != nil {
			return false
		}
		if !matched {
			return false
		}
	}
	return true
}

// QualifiesForDataPoint checks if the data point qualifies for the lookup
func (lc *LookupConfig) QualifiesForDataPoint(ctx context.Context, tCtx ottldatapoint.TransformContext) bool {
	if len(lc.MetricQualifiers) == 0 {
		return true
	}
	for _, qualifier := range lc.ParsedMetricQualifiers {
		matched, err := qualifier.Eval(ctx, tCtx)
		if err != nil {
			return false
		}
		if !matched {
			return false
		}
	}
	return true
}

type LookupTable []map[string]string

// Lookup function that searches for a row matching the tagName-value pairs and returns the value of the targetTagName column
func (lt LookupTable) Lookup(targetTagName string, logger *zap.Logger, conditions []string) (string, bool) {
	// If no conditions are provided, return the first occurrence of the targetTagName
	if len(conditions) == 0 {
		for _, row := range lt {
			if targetValue, exists := row[targetTagName]; exists {
				return targetValue, true
			}
		}
		return "", false
	}

	if len(conditions)%2 != 0 {
		logger.Warn("Invalid conditions: each tag name must be followed by a value")
		return "", false
	}

	for _, row := range lt {
		matches := true
		for i := 0; i < len(conditions); i += 2 {
			tagName := conditions[i]
			value := conditions[i+1]

			if row[tagName] != value {
				matches = false
				break
			}
		}

		if matches {
			targetValue, exists := row[targetTagName]
			if exists {
				return targetValue, true
			}
			return "", false
		}
	}

	return "", false
}
