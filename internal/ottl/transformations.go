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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
	"go.uber.org/multierr"
	"go.uber.org/zap"
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

type VendorID string

type RuleID string

type Transformations = transformations

type transformations struct {
	resourceTransformsByRuleId  map[VendorID]map[RuleID]resourceTransform
	scopeTransformsByRuleId     map[VendorID]map[RuleID]scopeTransform
	logTransformsByRuleId       map[VendorID]map[RuleID]logTransform
	spanTransformsByRuleId      map[VendorID]map[RuleID]spanTransform
	metricTransformsByRuleId    map[VendorID]map[RuleID]metricTransform
	dataPointTransformsByRuleId map[VendorID]map[RuleID]dataPointTransform
	logger                      *zap.Logger
}

func NewTransformations(logger *zap.Logger) transformations {
	return transformations{
		resourceTransformsByRuleId:  make(map[VendorID]map[RuleID]resourceTransform),
		scopeTransformsByRuleId:     make(map[VendorID]map[RuleID]scopeTransform),
		logTransformsByRuleId:       make(map[VendorID]map[RuleID]logTransform),
		spanTransformsByRuleId:      make(map[VendorID]map[RuleID]spanTransform),
		metricTransformsByRuleId:    make(map[VendorID]map[RuleID]metricTransform),
		dataPointTransformsByRuleId: make(map[VendorID]map[RuleID]dataPointTransform),
		logger:                      logger,
	}
}

func MergeWith(this transformations, other transformations) transformations {
	return transformations{
		resourceTransformsByRuleId:  merge(this.resourceTransformsByRuleId, other.resourceTransformsByRuleId),
		scopeTransformsByRuleId:     merge(this.scopeTransformsByRuleId, other.scopeTransformsByRuleId),
		logTransformsByRuleId:       merge(this.logTransformsByRuleId, other.logTransformsByRuleId),
		spanTransformsByRuleId:      merge(this.spanTransformsByRuleId, other.spanTransformsByRuleId),
		metricTransformsByRuleId:    merge(this.metricTransformsByRuleId, other.metricTransformsByRuleId),
		dataPointTransformsByRuleId: merge(this.dataPointTransformsByRuleId, other.dataPointTransformsByRuleId),
	}
}

func merge[T any](map1, map2 map[VendorID]map[RuleID]T) map[VendorID]map[RuleID]T {
	result := make(map[VendorID]map[RuleID]T, len(map1))

	// Copy all entries from map1
	for outerKey, innerMap := range map1 {
		result[outerKey] = make(map[RuleID]T, len(innerMap))
		for innerKey, value := range innerMap {
			result[outerKey][innerKey] = value
		}
	}

	// Merge map2 into the result
	for outerKey, innerMap := range map2 {
		// If outerKey already exists, merge the inner maps
		if _, exists := result[outerKey]; exists {
			for innerKey, value := range innerMap {
				result[outerKey][innerKey] = value
			}
		} else {
			// If outerKey does not exist, add the entire inner map
			result[outerKey] = make(map[RuleID]T, len(innerMap))
			for innerKey, value := range innerMap {
				result[outerKey][innerKey] = value
			}
		}
	}

	return result
}

func (t *transformations) Stop() {
	for _, logTransforms := range t.logTransformsByRuleId {
		for _, logTransform := range logTransforms {
			if logTransform.sampler != nil {
				_ = logTransform.sampler.Stop()
			}
		}
	}
	for _, spanTransforms := range t.spanTransformsByRuleId {
		for _, spanTransform := range spanTransforms {
			if spanTransform.sampler != nil {
				_ = spanTransform.sampler.Stop()
			}
		}
	}
}

func mkFactory[T any]() map[string]ottl.Factory[T] {
	factoryMap := map[string]ottl.Factory[T]{}
	for factoryName, factory := range ottlfuncs.StandardFuncs[T]() {
		factoryMap[factoryName] = factory
	}
	for factoryName, factory := range ottlfuncs.StandardConverters[T]() {
		factoryMap[factoryName] = factory
	}
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

func ParseTransformations(statement Instruction, logger *zap.Logger) (transformations, error) {
	var errors error

	contextStatements := statement.Statements
	resourceParser, _ := ottlresource.NewParser(mkFactory[ottlresource.TransformContext](), component.TelemetrySettings{Logger: logger})
	scopeParser, _ := ottlscope.NewParser(mkFactory[ottlscope.TransformContext](), component.TelemetrySettings{Logger: logger})
	logParser, _ := ottllog.NewParser(mkFactory[ottllog.TransformContext](), component.TelemetrySettings{Logger: logger})
	spanParser, _ := ottlspan.NewParser(mkFactory[ottlspan.TransformContext](), component.TelemetrySettings{Logger: logger})
	metricParser, _ := ottlmetric.NewParser(mkFactory[ottlmetric.TransformContext](), component.TelemetrySettings{Logger: logger})
	dataPointParser, _ := ottldatapoint.NewParser(mkFactory[ottldatapoint.TransformContext](), component.TelemetrySettings{Logger: logger})

	transformations := NewTransformations(logger)

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

			if _, exists := transformations.resourceTransformsByRuleId[statement.VendorId]; !exists {
				transformations.resourceTransformsByRuleId[statement.VendorId] = make(map[RuleID]resourceTransform)
			}
			transformations.resourceTransformsByRuleId[statement.VendorId][cs.RuleId] = resourceTransform{
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

			if _, exists := transformations.scopeTransformsByRuleId[statement.VendorId]; !exists {
				transformations.scopeTransformsByRuleId[statement.VendorId] = make(map[RuleID]scopeTransform)
			}
			transformations.scopeTransformsByRuleId[statement.VendorId][cs.RuleId] = scopeTransform{
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

			if _, exists := transformations.logTransformsByRuleId[statement.VendorId]; !exists {
				transformations.logTransformsByRuleId[statement.VendorId] = make(map[RuleID]logTransform)
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

			transformations.logTransformsByRuleId[statement.VendorId][cs.RuleId] = logTransform{
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

			if _, exists := transformations.spanTransformsByRuleId[statement.VendorId]; !exists {
				transformations.spanTransformsByRuleId[statement.VendorId] = make(map[RuleID]spanTransform)
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

			transformations.spanTransformsByRuleId[statement.VendorId][cs.RuleId] = spanTransform{
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

			if _, exists := transformations.metricTransformsByRuleId[statement.VendorId]; !exists {
				transformations.metricTransformsByRuleId[statement.VendorId] = make(map[RuleID]metricTransform)
			}
			transformations.metricTransformsByRuleId[statement.VendorId][cs.RuleId] = metricTransform{
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

			if _, exists := transformations.dataPointTransformsByRuleId[statement.VendorId]; !exists {
				transformations.dataPointTransformsByRuleId[statement.VendorId] = make(map[RuleID]dataPointTransform)
			}
			transformations.dataPointTransformsByRuleId[statement.VendorId][cs.RuleId] = dataPointTransform{
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

func evaluateTransform[T any](counter DeferrableCounter, rulesByRuleIdByVendorId map[VendorID]map[RuleID]T, vendorId VendorID, ruleIds pcommon.Slice, eval func(DeferrableCounter, T)) {
	if ruleIds.Len() == 0 || vendorId == "" {
		for _, transformsByRuleId := range rulesByRuleIdByVendorId {
			for _, transform := range transformsByRuleId {
				eval(counter, transform)
			}
		}
	} else {
		for i := 0; i < ruleIds.Len(); i++ {
			ruleId := ruleIds.At(i)
			if transform, ok := rulesByRuleIdByVendorId[vendorId][RuleID(ruleId.Str())]; ok {
				eval(counter, transform)
			}
		}
	}
}

func usableVendorId(vendorId VendorID) string {
	if vendorId == "" {
		return "_unset"
	}
	return string(vendorId)
}

func (t *transformations) ExecuteResourceTransforms(counter DeferrableCounter, transformCtx ottlresource.TransformContext, vendorId VendorID, ruleIds pcommon.Slice) {
	attrset := attribute.NewSet(attribute.String("context", "resource"), attribute.String("vendor_id", usableVendorId(vendorId)))
	CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "resource")))
	evaluateTransform[resourceTransform](counter, t.resourceTransformsByRuleId, vendorId, ruleIds, func(counter DeferrableCounter, resourceTransform resourceTransform) {
		allConditionsTrue := true
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition")))
		for _, condition := range resourceTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue {
			for _, statement := range resourceTransform.statements {
				_, _, err := statement.Execute(context.Background(), transformCtx)
				if err != nil {
					t.logger.Error("Error executing resource transformation", zap.Error(err))
				}
			}
		}
	})
}

func (t *transformations) ExecuteScopeTransforms(counter DeferrableCounter, transformCtx ottlscope.TransformContext, vendorId VendorID, ruleIds pcommon.Slice) {
	attrset := attribute.NewSet(attribute.String("context", "scope"), attribute.String("vendor_id", usableVendorId(vendorId)))
	CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "scope")))
	evaluateTransform[scopeTransform](counter, t.scopeTransformsByRuleId, vendorId, ruleIds, func(counter DeferrableCounter, scopeTransform scopeTransform) {
		allConditionsTrue := true
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition")))
		for _, condition := range scopeTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue {
			for _, statement := range scopeTransform.statements {
				_, _, err := statement.Execute(context.Background(), transformCtx)
				if err != nil {
					t.logger.Error("Error executing scope transformation", zap.Error(err))
				}
			}
		}
	})
}

func (t *transformations) ExecuteLogTransforms(counter DeferrableCounter, transformCtx ottllog.TransformContext, vendorId VendorID, ruleIds pcommon.Slice) {
	attrset := attribute.NewSet(attribute.String("context", "log"), attribute.String("vendor_id", usableVendorId(vendorId)))
	CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "log")))
	evaluateTransform[logTransform](counter, t.logTransformsByRuleId, vendorId, ruleIds, func(counter DeferrableCounter, logTransform logTransform) {
		allConditionsTrue := true
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition")))
		for _, condition := range logTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-sampler"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue && logTransform.sampler != nil {
			serviceName := GetServiceName(transformCtx.GetResource())
			fingerprint, exists := transformCtx.GetLogRecord().Attributes().Get(translate.CardinalFieldFingerprint)
			if !exists {
				return
			}
			key := fmt.Sprintf("%s:%s", serviceName, fingerprint.AsString())
			sampleRate := logTransform.sampler.GetSampleRate(key)
			allConditionsTrue = allConditionsTrue && shouldFilter(sampleRate, rand.Float64())
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue {
			for _, statement := range logTransform.statements {
				_, _, err := statement.Execute(context.Background(), transformCtx)
				if err != nil {
					t.logger.Error("Error executing log transformation", zap.Error(err))
				}
			}
		}
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

func (t *transformations) ExecuteSpanTransforms(counter DeferrableCounter, transformCtx ottlspan.TransformContext, vendorId VendorID, ruleIds pcommon.Slice) {
	attrset := attribute.NewSet(attribute.String("context", "span"), attribute.String("vendor_id", usableVendorId(vendorId)))
	CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "span")))
	evaluateTransform[spanTransform](counter, t.spanTransformsByRuleId, vendorId, ruleIds, func(counter DeferrableCounter, spanTransform spanTransform) {
		allConditionsTrue := true
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition")))
		for _, condition := range spanTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-sampler"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue && spanTransform.sampler != nil {
			randval := rand.Float64()
			serviceName := GetServiceName(transformCtx.GetResource())
			fingerprint, exists := transformCtx.GetSpan().Attributes().Get(translate.CardinalFieldFingerprint)
			if !exists {
				return
			}
			key := fmt.Sprintf("%s:%s", serviceName, fingerprint.AsString())
			sampleRate := spanTransform.sampler.GetSampleRate(key)
			allConditionsTrue = allConditionsTrue && shouldFilter(sampleRate, randval)
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue {
			for _, statement := range spanTransform.statements {
				_, _, err := statement.Execute(context.Background(), transformCtx)
				if err != nil {
					t.logger.Error("Error executing span transformation", zap.Error(err))
				}
			}
		}
	})
}

func (t *transformations) ExecuteMetricTransforms(counter DeferrableCounter, transformCtx ottlmetric.TransformContext, vendorId VendorID, ruleIds pcommon.Slice) {
	attrset := attribute.NewSet(attribute.String("context", "metric"), attribute.String("vendor_id", usableVendorId(vendorId)))
	CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "metric")))
	evaluateTransform[metricTransform](counter, t.metricTransformsByRuleId, vendorId, ruleIds, func(counter DeferrableCounter, metricTransform metricTransform) {
		allConditionsTrue := true
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition")))
		for _, condition := range metricTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue {
			for _, statement := range metricTransform.statements {
				_, _, err := statement.Execute(context.Background(), transformCtx)
				if err != nil {
					t.logger.Error("Error executing metric transformation", zap.Error(err))
				}
			}
		}
	})
}

func (t *transformations) ExecuteDataPointTransforms(counter DeferrableCounter, transformCtx ottldatapoint.TransformContext, vendorId VendorID, ruleIds pcommon.Slice) {
	attrset := attribute.NewSet(attribute.String("context", "datapoint"), attribute.String("vendor_id", usableVendorId(vendorId)))
	CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "datapoint")))
	evaluateTransform[dataPointTransform](counter, t.dataPointTransformsByRuleId, vendorId, ruleIds, func(counter DeferrableCounter, dataPointTransform dataPointTransform) {
		allConditionsTrue := true
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-condition")))
		for _, condition := range dataPointTransform.conditions {
			conditionMet, _ := condition.Eval(context.Background(), transformCtx)
			allConditionsTrue = allConditionsTrue && conditionMet
		}
		CounterAdd(counter, 1, metric.WithAttributeSet(attrset), metric.WithAttributes(attribute.String("stage", "pre-statements"), attribute.Bool("all_conditions_true", allConditionsTrue)))
		if allConditionsTrue {
			for _, statement := range dataPointTransform.statements {
				_, _, err := statement.Execute(context.Background(), transformCtx)
				if err != nil {
					t.logger.Error("Error executing datapoint transformation", zap.Error(err))
				}
			}
		}
	})
}
