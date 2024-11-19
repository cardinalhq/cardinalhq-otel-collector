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
	"math/rand"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/ottl/functions"
	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/telemetry"
	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/translate"
)

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
	//samplerConfig SamplingConfig
	sampler Sampler
}

type spanTransform struct {
	context    ContextID
	conditions []*ottl.Condition[ottlspan.TransformContext]
	statements []*ottl.Statement[ottlspan.TransformContext]
	//samplerConfig SamplingConfig
	sampler Sampler
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

type RuleID string

type Transformations = transformations

type transformations struct {
	resourceTransforms  map[RuleID]resourceTransform
	scopeTransforms     map[RuleID]scopeTransform
	logTransforms       map[RuleID]logTransform
	spanTransforms      map[RuleID]spanTransform
	metricTransforms    map[RuleID]metricTransform
	dataPointTransforms map[RuleID]dataPointTransform
}

func NewTransformations() *transformations {
	return &transformations{
		resourceTransforms:  make(map[RuleID]resourceTransform),
		scopeTransforms:     make(map[RuleID]scopeTransform),
		logTransforms:       make(map[RuleID]logTransform),
		spanTransforms:      make(map[RuleID]spanTransform),
		metricTransforms:    make(map[RuleID]metricTransform),
		dataPointTransforms: make(map[RuleID]dataPointTransform),
	}
}

func MergeWith(this *transformations, other *transformations) *transformations {
	return &transformations{
		resourceTransforms:  merge(this.resourceTransforms, other.resourceTransforms),
		scopeTransforms:     merge(this.scopeTransforms, other.scopeTransforms),
		logTransforms:       merge(this.logTransforms, other.logTransforms),
		spanTransforms:      merge(this.spanTransforms, other.spanTransforms),
		metricTransforms:    merge(this.metricTransforms, other.metricTransforms),
		dataPointTransforms: merge(this.dataPointTransforms, other.dataPointTransforms),
	}
}

func merge[T any](map1, map2 map[RuleID]T) map[RuleID]T {
	result := make(map[RuleID]T, len(map1))

	for key, value := range map1 {
		result[key] = value
	}
	for key, value := range map2 {
		result[key] = value
	}

	return result
}

func (t *transformations) Stop() {
	for _, logTransform := range t.logTransforms {
		if logTransform.sampler != nil {
			_ = logTransform.sampler.Stop()
		}
	}
	for _, spanTransform := range t.spanTransforms {
		if spanTransform.sampler != nil {
			_ = spanTransform.sampler.Stop()
		}
	}
}

type valueArguments[K any] struct {
	Target ottl.Getter[K] `ottlarg:"0"`
}

func newValueFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("value", &valueArguments[K]{}, createValueFunction[K])
}

func createValueFunction[K any](_ ottl.FunctionContext, a ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := a.(*valueArguments[K])
	if !ok {
		return nil, fmt.Errorf("valueFactory args must be of type *valueArguments[K]")
	}

	return valueFn[K](args)
}

func valueFn[K any](c *valueArguments[K]) (ottl.ExprFunc[K], error) {
	return func(ctx context.Context, tCtx K) (interface{}, error) {
		return c.Target.Get(ctx, tCtx)
	}, nil
}

func ToFactory[T any]() map[string]ottl.Factory[T] {
	factoryMap := map[string]ottl.Factory[T]{}
	for factoryName, factory := range ottlfuncs.StandardFuncs[T]() {
		factoryMap[factoryName] = factory
	}
	for factoryName, factory := range ottlfuncs.StandardConverters[T]() {
		factoryMap[factoryName] = factory
	}
	for factoryName, factory := range functions.CustomFunctions[T]() {
		factoryMap[factoryName] = factory
	}

	valueFactory := newValueFactory[T]()
	factoryMap[valueFactory.Name()] = valueFactory

	return factoryMap
}

func createSampler(c SamplingConfig) Sampler {
	if c.RPS > 0 {
		return NewRPSSampler(WithMaxRPS(c.RPS))
	}
	if c.SampleRate > 0 {
		return NewStaticSampler(int(1 / c.SampleRate))
	}
	return nil
}

func GetServiceName(resource pcommon.Resource) string {
	r := resource.Attributes()
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func ParseTransformations(logger *zap.Logger, statements []ContextStatement) (*transformations, error) {
	var errors error

	resourceParser, _ := ottlresource.NewParser(ToFactory[ottlresource.TransformContext](), component.TelemetrySettings{Logger: logger})
	scopeParser, _ := ottlscope.NewParser(ToFactory[ottlscope.TransformContext](), component.TelemetrySettings{Logger: logger})
	logParser, _ := ottllog.NewParser(ToFactory[ottllog.TransformContext](), component.TelemetrySettings{Logger: logger})
	spanParser, _ := ottlspan.NewParser(ToFactory[ottlspan.TransformContext](), component.TelemetrySettings{Logger: logger})
	metricParser, _ := ottlmetric.NewParser(ToFactory[ottlmetric.TransformContext](), component.TelemetrySettings{Logger: logger})
	dataPointParser, _ := ottldatapoint.NewParser(ToFactory[ottldatapoint.TransformContext](), component.TelemetrySettings{Logger: logger})

	transformations := NewTransformations()

	for _, cs := range statements {
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

			transformations.resourceTransforms[cs.RuleId] = resourceTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			}

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

			transformations.scopeTransforms[cs.RuleId] = scopeTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			}

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

			s := createSampler(cs.SamplingConfig)
			if s != nil {
				err = s.Start()
				if err != nil {
					logger.Error("Error starting sampler", zap.Error(err))
					errors = multierr.Append(errors, err)
					continue
				}
			}

			transformations.logTransforms[cs.RuleId] = logTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
				sampler:    s,
			}

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

			s := createSampler(cs.SamplingConfig)
			if s != nil {
				err = s.Start()
				if err != nil {
					logger.Error("Error starting sampler", zap.Error(err))
					errors = multierr.Append(errors, err)
					continue
				}
			}

			transformations.spanTransforms[cs.RuleId] = spanTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
				sampler:    s,
			}

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

			transformations.metricTransforms[cs.RuleId] = metricTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			}

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

			transformations.dataPointTransforms[cs.RuleId] = dataPointTransform{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			}

		default:
			logger.Error("Unknown context: ", zap.String("context", string(cs.Context)))
		}
	}

	return transformations, errors
}

func evaluateTransform[T any](counter telemetry.DeferrableCounter, rules map[RuleID]T, eval func(telemetry.DeferrableCounter, T, string)) {
	for ruleID, transform := range rules {
		eval(counter, transform, string(ruleID))
	}
}

func (t *transformations) ExecuteResourceTransforms(logger *zap.Logger, counter telemetry.DeferrableCounter, errorCounter telemetry.DeferrableCounter, histogram telemetry.DeferrableHistogram, transformCtx ottlresource.TransformContext) {
	attrset := attribute.NewSet(attribute.String("context", "resource"))
	startTime := time.Now()
	telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "resource")))
	evaluateTransform[resourceTransform](counter, t.resourceTransforms, func(counter telemetry.DeferrableCounter, resourceTransform resourceTransform, ruleID string) {
		allConditionsTrue := true
		for _, condition := range resourceTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if !allConditionsTrue {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, false),
				attribute.Bool(translate.SamplerAllowed, false),
				attribute.Bool(translate.ConditionsMatched, false),
			))
			return
		}
		for _, statement := range resourceTransform.statements {
			_, _, err := statement.Execute(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "statementEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing resource transformation", zap.Error(err))
			}
		}
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, true),
				attribute.Bool(translate.SamplerAllowed, true),
				attribute.Bool(translate.ConditionsMatched, true)))

		telemetry.HistogramAdd(histogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID)))
	})
}

func (t *transformations) ExecuteScopeTransforms(logger *zap.Logger, counter telemetry.DeferrableCounter, errorCounter telemetry.DeferrableCounter, histogram telemetry.DeferrableHistogram, transformCtx ottlscope.TransformContext) {
	attrset := attribute.NewSet(attribute.String("context", "scope"))
	startTime := time.Now()
	telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "scope")))
	evaluateTransform[scopeTransform](counter, t.scopeTransforms, func(counter telemetry.DeferrableCounter, scopeTransform scopeTransform, ruleID string) {
		allConditionsTrue := true
		for _, condition := range scopeTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if !allConditionsTrue {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, false),
				attribute.Bool(translate.SamplerAllowed, false),
				attribute.Bool(translate.ConditionsMatched, false),
			))
			return
		}

		for _, statement := range scopeTransform.statements {
			_, _, err := statement.Execute(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "statementEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing scope transformation", zap.Error(err))
			}
		}
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, true),
				attribute.Bool(translate.SamplerAllowed, true),
				attribute.Bool(translate.ConditionsMatched, true)))

		telemetry.HistogramAdd(histogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID)))
	})
}

func (t *transformations) ExecuteLogTransforms(logger *zap.Logger, counter telemetry.DeferrableCounter, errorCounter telemetry.DeferrableCounter, histogram telemetry.DeferrableHistogram, transformCtx ottllog.TransformContext) {
	attrset := attribute.NewSet(attribute.String("context", "log"))
	startTime := time.Now()

	telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "log")))
	evaluateTransform[logTransform](counter, t.logTransforms, func(counter telemetry.DeferrableCounter, logTransform logTransform, ruleID string) {
		allConditionsTrue := true
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition"), attribute.String("rule_id", ruleID)))
		for _, condition := range logTransform.conditions {
			conditionMet, err := condition.Eval(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "conditionEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
			}
			allConditionsTrue = allConditionsTrue && conditionMet
		}

		if !allConditionsTrue {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, false),
				attribute.Bool(translate.SamplerAllowed, false),
				attribute.Bool(translate.ConditionsMatched, false),
			))
			return
		}
		var shouldAllow = true
		if logTransform.sampler != nil {
			serviceName := GetServiceName(transformCtx.GetResource())
			fingerprint, exists := transformCtx.GetLogRecord().Attributes().Get(translate.CardinalFieldFingerprint)
			if !exists {
				return
			}
			key := fmt.Sprintf("%s:%s", serviceName, fingerprint.AsString())
			sampleRate := logTransform.sampler.GetSampleRate(key)
			shouldAllow = shouldFilter(sampleRate, rand.Float64())
		}
		if !shouldAllow {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.SamplerAllowed, false),
				attribute.Bool(translate.ConditionsMatched, true),
				attribute.Bool(translate.StatementsEvaluated, false),
			))
			return
		}
		for _, statement := range logTransform.statements {
			_, _, err := statement.Execute(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "statementEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing log transformation", zap.Error(err))
			}
		}

		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, true),
				attribute.Bool(translate.SamplerAllowed, true),
				attribute.Bool(translate.ConditionsMatched, true)))

		telemetry.HistogramAdd(histogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID)))
	})
}

func shouldFilter(rate int, randval float64) bool {
	switch rate {
	case 0:
		return true
	case 1:
		return false
	default:
		return randval > 1/float64(rate)
	}
}

func (t *transformations) ExecuteSpanTransforms(logger *zap.Logger, counter telemetry.DeferrableCounter, errorCounter telemetry.DeferrableCounter, histogram telemetry.DeferrableHistogram, transformCtx ottlspan.TransformContext) {
	attrset := attribute.NewSet(attribute.String("context", "span"))

	evaluateTransform[spanTransform](counter, t.spanTransforms, func(counter telemetry.DeferrableCounter, spanTransform spanTransform, ruleID string) {
		startTime := time.Now()

		allConditionsTrue := true
		for _, condition := range spanTransform.conditions {
			conditionMet, err := condition.Eval(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "conditionEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing span conditions", zap.Error(err))
			}
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		if !allConditionsTrue {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, false),
				attribute.Bool(translate.SamplerAllowed, false),
				attribute.Bool(translate.ConditionsMatched, false),
			))
			return
		}

		var shouldAllow = true
		if spanTransform.sampler != nil {
			randval := rand.Float64()
			serviceName := GetServiceName(transformCtx.GetResource())
			fingerprint, exists := transformCtx.GetSpan().Attributes().Get(translate.CardinalFieldFingerprint)
			if !exists {
				return
			}
			key := fmt.Sprintf("%s:%s", serviceName, fingerprint.AsString())
			sampleRate := spanTransform.sampler.GetSampleRate(key)
			shouldAllow = shouldFilter(sampleRate, randval)
		}

		if !shouldAllow {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.SamplerAllowed, false),
				attribute.Bool(translate.ConditionsMatched, true),
				attribute.Bool(translate.StatementsEvaluated, false),
			))
			return
		}

		for _, statement := range spanTransform.statements {
			_, _, err := statement.Execute(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "statementEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing span transformation", zap.Error(err))
			}
		}
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, true),
				attribute.Bool(translate.SamplerAllowed, true),
				attribute.Bool(translate.ConditionsMatched, true)))

		telemetry.HistogramAdd(histogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID)))
	})
}

func (t *transformations) ExecuteMetricTransforms(logger *zap.Logger, counter telemetry.DeferrableCounter, errorCounter telemetry.DeferrableCounter, histogram telemetry.DeferrableHistogram, transformCtx ottlmetric.TransformContext) {
	attrset := attribute.NewSet(attribute.String("context", "metric"))
	startTime := time.Now()

	telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "metric")))
	evaluateTransform[metricTransform](counter, t.metricTransforms, func(counter telemetry.DeferrableCounter, metricTransform metricTransform, ruleID string) {
		allConditionsTrue := true
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition"), attribute.String("rule_id", ruleID)))
		for _, condition := range metricTransform.conditions {
			conditionMet, err := condition.Eval(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "conditionEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing metric conditions", zap.Error(err))
			}
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.String("rule_id", ruleID), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if !allConditionsTrue {
			return
		}
		for _, statement := range metricTransform.statements {
			_, _, err := statement.Execute(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "statementEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing metric transformation", zap.Error(err))
			}
		}
		telemetry.HistogramAdd(histogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID)))
	})
}

func (t *transformations) ExecuteDatapointTransforms(logger *zap.Logger, counter telemetry.DeferrableCounter, errorCounter telemetry.DeferrableCounter, histogram telemetry.DeferrableHistogram, transformCtx ottldatapoint.TransformContext) {
	attrset := attribute.NewSet(attribute.String("context", "datapoint"))
	startTime := time.Now()

	telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "datapoint")))
	evaluateTransform[dataPointTransform](counter, t.dataPointTransforms, func(counter telemetry.DeferrableCounter, dataPointTransform dataPointTransform, ruleID string) {
		allConditionsTrue := true
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition"), attribute.String("rule_id", ruleID)))
		for _, condition := range dataPointTransform.conditions {
			conditionMet, err := condition.Eval(context.Background(), transformCtx)
			if err != nil {
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "conditionEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
				logger.Error("Error executing span conditions", zap.Error(err))
			}
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.String("rule_id", ruleID), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if !allConditionsTrue {
			telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
				attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, false),
				attribute.Bool(translate.ConditionsMatched, false),
			))
			return
		}
		for _, statement := range dataPointTransform.statements {
			_, _, err := statement.Execute(context.Background(), transformCtx)
			if err != nil {
				logger.Error("Error executing datapoint transformation", zap.Error(err))
				telemetry.CounterAdd(errorCounter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(
					attribute.String(translate.RuleId, ruleID),
					attribute.String(translate.Stage, "statementEval"),
					attribute.String(translate.ErrorMsg, err.Error()),
				))
			}
		}
		telemetry.CounterAdd(counter, 1, metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String(translate.RuleId, ruleID),
				attribute.Bool(translate.StatementsEvaluated, true),
				attribute.Bool(translate.ConditionsMatched, true)))

		telemetry.HistogramAdd(histogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String("rule_id", ruleID)))
	})
}
