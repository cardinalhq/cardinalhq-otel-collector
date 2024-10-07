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
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type ContextID string

type ContextStatement struct {
	Context    ContextID `mapstructure:"context"`
	Conditions []string  `mapstructure:"conditions"`
	Statements []string  `mapstructure:"statements"`
}

type resourceTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottlresource.TransformContext]
	statements []*ottl.Statement[ottlresource.TransformContext]
}

type scopeTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottlscope.TransformContext]
	statements []*ottl.Statement[ottlscope.TransformContext]
}

type logTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottllog.TransformContext]
	statements []*ottl.Statement[ottllog.TransformContext]
}

type spanTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottlspan.TransformContext]
	statements []*ottl.Statement[ottlspan.TransformContext]
}

type metricTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottlmetric.TransformContext]
	statements []*ottl.Statement[ottlmetric.TransformContext]
}

type dataPointTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottldatapoint.TransformContext]
	statements []*ottl.Statement[ottldatapoint.TransformContext]
}

type Transformations struct {
	resourceTransforms  []resourceTransform
	scopeTransforms     []scopeTransform
	logTransforms       []logTransform
	spanTransforms      []spanTransform
	metricTransforms    []metricTransform
	dataPointTransforms []dataPointTransform
}

func ParseTransformations(contextStatements []ContextStatement, logger *zap.Logger) (Transformations, error) {
	var errors error

	resourceParser, _ := ottlresource.NewParser(ottlfuncs.StandardFuncs[ottlresource.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	scopeParser, _ := ottlscope.NewParser(ottlfuncs.StandardFuncs[ottlscope.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	logParser, _ := ottllog.NewParser(ottlfuncs.StandardFuncs[ottllog.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	spanParser, _ := ottlspan.NewParser(ottlfuncs.StandardFuncs[ottlspan.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	metricParser, _ := ottlmetric.NewParser(ottlfuncs.StandardFuncs[ottlmetric.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	dataPointParser, _ := ottldatapoint.NewParser(ottlfuncs.StandardFuncs[ottldatapoint.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})

	transformations := Transformations{
		resourceTransforms:  []resourceTransform{},
		scopeTransforms:     []scopeTransform{},
		logTransforms:       []logTransform{},
		spanTransforms:      []spanTransform{},
		metricTransforms:    []metricTransform{},
		dataPointTransforms: []dataPointTransform{},
	}

	for _, cs := range contextStatements {
		switch cs.Context {
		case "resource":
			conditions, err := resourceParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing resource conditions", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}
			statements, err := resourceParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing resource statements", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}

			transformations.resourceTransforms = append(transformations.resourceTransforms, resourceTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "scope":
			conditions, err := scopeParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing scope conditions", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}
			statements, err := scopeParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing scope statements", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}

			transformations.scopeTransforms = append(transformations.scopeTransforms, scopeTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "log":
			conditions, err := logParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing log conditions", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}
			statements, err := logParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing log statements", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}

			transformations.logTransforms = append(transformations.logTransforms, logTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "span":
			conditions, err := spanParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing span conditions", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}
			statements, err := spanParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing span statements", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}

			transformations.spanTransforms = append(transformations.spanTransforms, spanTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "metric":
			conditions, err := metricParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing metric conditions", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}
			statements, err := metricParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing metric statements", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}

			transformations.metricTransforms = append(transformations.metricTransforms, metricTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "datapoint":
			conditions, err := dataPointParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing datapoint conditions", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}
			statements, err := dataPointParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing datapoint statements", zap.Error(err))
				errors = multierr.Append(errors, err)
				continue
			}

			transformations.dataPointTransforms = append(transformations.dataPointTransforms, dataPointTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		default:
			logger.Error("Unknown context: ", zap.String("context", string(cs.Context)))
		}
	}

	return transformations, errors
}

func (t *Transformations) ExecuteResourceLogTransforms(rl plog.ResourceLogs) {
	transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
	for _, transformation := range t.resourceTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					rl.Resource().Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteScopeLogTransforms(sl plog.ScopeLogs, rl plog.ResourceLogs) {
	transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
	for _, transformation := range t.scopeTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					sl.Scope().Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteLogTransforms(log plog.LogRecord, sl plog.ScopeLogs, rl plog.ResourceLogs) {
	transformCtx := ottllog.NewTransformContext(log, sl.Scope(), rl.Resource(), sl, rl)
	for _, transformation := range t.logTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					log.Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteResourceSpanTransformations(rl ptrace.ResourceSpans) {
	transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
	for _, transformation := range t.resourceTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					rl.Resource().Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteScopeSpanTransformations(sl ptrace.ScopeSpans, rl ptrace.ResourceSpans) {
	transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
	for _, transformation := range t.scopeTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					sl.Scope().Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteSpanTransformations(span ptrace.Span, sl ptrace.ScopeSpans, rl ptrace.ResourceSpans) {
	transformCtx := ottlspan.NewTransformContext(span, sl.Scope(), rl.Resource(), sl, rl)
	for _, transformation := range t.spanTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					span.Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteResourceMetricTransforms(rl pmetric.ResourceMetrics) {
	transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
	for _, transformation := range t.resourceTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					rl.Resource().Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteScopeMetricTransforms(sl pmetric.ScopeMetrics, rl pmetric.ResourceMetrics) {
	transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
	for _, transformation := range t.scopeTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					sl.Scope().Attributes().PutBool(translate.CardinalFieldTransformed, true)
				}
			}
		}
	}
}

func (t *Transformations) ExecuteMetricTransforms(m pmetric.Metric,
	sl pmetric.ScopeMetrics,
	rl pmetric.ResourceMetrics) {
	transformCtx := ottlmetric.NewTransformContext(m, sl.Metrics(), sl.Scope(), rl.Resource(), sl, rl)
	for _, transformation := range t.metricTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, _, _ = statement.Execute(context.Background(), transformCtx)
			}
		}
	}
}

func (t *Transformations) ExecuteAllMetricsTransforms(d any,
	m pmetric.Metric,
	sl pmetric.ScopeMetrics,
	rl pmetric.ResourceMetrics) {
	t.ExecuteResourceMetricTransforms(rl)
	t.ExecuteScopeMetricTransforms(sl, rl)
	t.ExecuteMetricTransforms(m, sl, rl)
	t.ExecuteDataPointTransforms(d, m, sl, rl)
}

func (t *Transformations) ExecuteDataPointTransforms(d any,
	m pmetric.Metric,
	sl pmetric.ScopeMetrics,
	rl pmetric.ResourceMetrics) {
	transformCtx := ottldatapoint.NewTransformContext(d, m, sl.Metrics(), sl.Scope(), rl.Resource(), sl, rl)
	for _, transformation := range t.dataPointTransforms {
		allConditionsTrue := true
		for _, condition := range transformation.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if allConditionsTrue {
			for _, statement := range transformation.statements {
				_, transformed, _ := statement.Execute(context.Background(), transformCtx)
				if transformed {
					switch d.(type) {
					case pmetric.NumberDataPoint:
						attrs := d.(pmetric.NumberDataPoint).Attributes()
						attrs.PutBool(translate.CardinalFieldTransformed, true)
					case pmetric.HistogramDataPoint:
						attrs := d.(pmetric.HistogramDataPoint).Attributes()
						attrs.PutBool(translate.CardinalFieldTransformed, true)
					case pmetric.ExponentialHistogramDataPoint:
						attrs := d.(pmetric.ExponentialHistogramDataPoint).Attributes()
						attrs.PutBool(translate.CardinalFieldTransformed, true)
					case pmetric.SummaryDataPoint:
						attrs := d.(pmetric.SummaryDataPoint).Attributes()
						attrs.PutBool(translate.CardinalFieldTransformed, true)
					}
				}
			}
		}
	}
}
