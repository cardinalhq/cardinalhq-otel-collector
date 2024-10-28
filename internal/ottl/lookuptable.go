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
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// LookupKey represents a condition to be matched in the lookup table.
// So for example, go get the value of ColumnName = serviceName (derived by executing the OTTLExpression say: resource.attributes["service.name"]) = service1
// Now find the record in the lookup table where serviceName = service1.
type LookupKey struct {
	ColumnName string `json:"column_name,omitempty" yaml:"column_name,omitempty"`
	Expression string `json:"expression,omitempty" yaml:"expression,omitempty"`

	parsedLogExpression    *ottl.Statement[ottllog.TransformContext]
	parsedSpanExpression   *ottl.Statement[ottlspan.TransformContext]
	parsedMetricExpression *ottl.Statement[ottldatapoint.TransformContext]
}

//TODO: Read the LookupTable from RDS on the `external_api` and only send the `TransposedLookupTable` down to the collector.

type LookupTable []map[string]string

type transposedLookupTable map[string]map[string]string

type LookupRule struct {
	FieldNamesToSet []string     `json:"field_names_to_set,omitempty" yaml:"field_names_to_set,omitempty"`
	Keys            []*LookupKey `json:"keys,omitempty" yaml:"keys,omitempty"`

	transposed *transposedLookupTable // make a special transposed table for this rule, to speed up lookups.
}

type LookupConfig struct {
	TableName   string        `json:"table_name,omitempty" yaml:"table_name,omitempty"`
	LookupTable LookupTable   `json:"lookup_table,omitempty" yaml:"lookup_table,omitempty"`
	LogRules    []*LookupRule `json:"log_rules,omitempty" yaml:"log_rules,omitempty"`
	SpanRules   []*LookupRule `json:"span_rules,omitempty" yaml:"span_rules,omitempty"`
	MetricRules []*LookupRule `json:"metric_rules,omitempty" yaml:"metric_rules,omitempty"`
}

// Helper function to create a key for the transposed map from conditions
func createKey(conditions []string) string {
	return strings.Join(conditions, "|")
}

// Transpose dynamically converts a regular LookupTable into a TransposedLookupTable using the provided condition columns
func (lt LookupTable) Transpose(conditionColumns []string) *transposedLookupTable {
	transposed := transposedLookupTable{}

	for _, row := range lt {
		conditions := make([]string, 0)
		for _, column := range conditionColumns {
			conditions = append(conditions, column)
			conditions = append(conditions, row[column])
		}

		key := createKey(conditions)
		transposed[key] = row
	}

	return &transposed
}

// Lookup Optimized Lookup function for TransposedLookupTable using dynamic keys
func (tlt transposedLookupTable) Lookup(targetTagName string, conditions []string) (string, bool) {
	// Create the lookup key from the conditions
	key := createKey(conditions)

	if row, exists := tlt[key]; exists {
		if targetValue, exists := row[targetTagName]; exists {
			return targetValue, true
		}
	}

	return "", false
}

func (lc *LookupConfig) Init(logger *zap.Logger) {
	if len(lc.LogRules) > 0 {
		logParser, _ := ottllog.NewParser(ToFactory[ottllog.TransformContext](), component.TelemetrySettings{Logger: logger})
		conditionColumns := make([]string, 0)

		for _, logRule := range lc.LogRules {
			for _, key := range logRule.Keys {
				parsedLogExpression, err := logParser.ParseStatement(fmt.Sprintf("value(%s)", key.Expression))
				if err != nil {
					logger.Error("Error parsing log expression", zap.Error(err))
					return
				}
				conditionColumns = append(conditionColumns, key.ColumnName)
				key.parsedLogExpression = parsedLogExpression
			}
			logRule.transposed = lc.LookupTable.Transpose(conditionColumns)
		}
	}

	// for spans
	if len(lc.SpanRules) > 0 {
		spanParser, _ := ottlspan.NewParser(ToFactory[ottlspan.TransformContext](), component.TelemetrySettings{Logger: logger})
		conditionColumns := make([]string, 0)

		for _, spanRule := range lc.SpanRules {
			for _, key := range spanRule.Keys {
				parsedSpanExpression, err := spanParser.ParseStatement(fmt.Sprintf("value(%s)", key.Expression))
				if err != nil {
					logger.Error("Error parsing span expression", zap.Error(err))
					return
				}
				conditionColumns = append(conditionColumns, key.ColumnName)
				key.parsedSpanExpression = parsedSpanExpression
			}
			spanRule.transposed = lc.LookupTable.Transpose(conditionColumns)
		}
	}

	// for metric data points
	if len(lc.MetricRules) > 0 {
		metricsParser, _ := ottldatapoint.NewParser(ToFactory[ottldatapoint.TransformContext](), component.TelemetrySettings{Logger: logger})
		conditionColumns := make([]string, 0)
		for _, metricsRule := range lc.SpanRules {
			for _, key := range metricsRule.Keys {
				parsedMetricsExpression, err := metricsParser.ParseStatement(fmt.Sprintf("value(%s)", key.Expression))
				if err != nil {
					logger.Error("Error parsing metrics expression", zap.Error(err))
					return
				}
				conditionColumns = append(conditionColumns, key.ColumnName)
				key.parsedMetricExpression = parsedMetricsExpression
			}
			metricsRule.transposed = lc.LookupTable.Transpose(conditionColumns)
		}
	}
}

// ExecuteLogsRules executes the log rules for the given record
func (lc *LookupConfig) ExecuteLogsRules(ctx context.Context, tCtx ottllog.TransformContext, record plog.LogRecord) {
	for _, lr := range lc.LogRules {
		conditionsArray := make([]string, 0, len(lr.Keys)*2)

		for _, lookupCondition := range lr.Keys {
			expression := lookupCondition.parsedLogExpression
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
		if len(lr.Keys) > 0 && len(conditionsArray) == 0 {
			return
		}
		for _, fieldNameToSet := range lr.FieldNamesToSet {
			targetValue, found := lr.transposed.Lookup(fieldNameToSet, conditionsArray)
			if found {
				record.Attributes().PutStr(fieldNameToSet, targetValue)
			}
		}
	}
}

// ExecuteSpansRules executes the span rules for the given record
func (lc *LookupConfig) ExecuteSpansRules(ctx context.Context, tCtx ottlspan.TransformContext, record ptrace.Span) {
	for _, lr := range lc.SpanRules {
		conditionsArray := make([]string, 0, len(lr.Keys)*2)

		for _, lookupCondition := range lr.Keys {
			expression := lookupCondition.parsedSpanExpression
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
		if len(lr.Keys) > 0 && len(conditionsArray) == 0 {
			return
		}
		for _, fieldNameToSet := range lr.FieldNamesToSet {
			targetValue, found := lr.transposed.Lookup(fieldNameToSet, conditionsArray)
			if found {
				record.Attributes().PutStr(fieldNameToSet, targetValue)
			}
		}
	}
}

// ExecuteMetricsRules executes the metrics rules for the given record
func (lc *LookupConfig) ExecuteMetricsRules(ctx context.Context, tCtx ottldatapoint.TransformContext, handlerFunc func(tagToSet string, targetValue string)) {
	for _, lr := range lc.MetricRules {
		conditionsArray := make([]string, 0, len(lr.Keys)*2)

		for _, lookupCondition := range lr.Keys {
			expression := lookupCondition.parsedMetricExpression
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
		if len(lr.Keys) > 0 && len(conditionsArray) == 0 {
			return
		}
		for _, fieldNameToSet := range lr.FieldNamesToSet {
			targetValue, found := lr.transposed.Lookup(fieldNameToSet, conditionsArray)
			if found {
				handlerFunc(fieldNameToSet, targetValue)
			}
		}
	}
}
