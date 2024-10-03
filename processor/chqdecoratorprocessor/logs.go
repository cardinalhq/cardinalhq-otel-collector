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

package chqdecoratorprocessor

import (
	"context"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
	"os"
)

type resourceTransformation struct {
	context    ContextID
	conditions []*ottl.Condition[ottlresource.TransformContext]
	statements []*ottl.Statement[ottlresource.TransformContext]
}

type scopeTransformation struct {
	context    ContextID
	conditions []*ottl.Condition[ottlscope.TransformContext]
	statements []*ottl.Statement[ottlscope.TransformContext]
}

type logTransformation struct {
	context    ContextID
	conditions []*ottl.Condition[ottllog.TransformContext]
	statements []*ottl.Statement[ottllog.TransformContext]
}

type transformations struct {
	resourceTransformations []resourceTransformation
	scopeTransformations    []scopeTransformation
	logTransformations      []logTransformation
}

type logProcessor struct {
	logger  *zap.Logger
	finger  fingerprinter.Fingerprinter
	podName string

	transformations transformations
}

func (lp *logProcessor) toTransformations(contextStatements []ContextStatement) transformations {
	// Set up parsers for each context type (resource, scope, log)
	resourceParser, _ := ottlresource.NewParser(ottlfuncs.StandardFuncs[ottlresource.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	scopeParser, _ := ottlscope.NewParser(ottlfuncs.StandardFuncs[ottlscope.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	logParser, _ := ottllog.NewParser(ottlfuncs.StandardFuncs[ottllog.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})

	// Initialize an empty slice for each type of transformation
	transformations := transformations{
		resourceTransformations: []resourceTransformation{},
		scopeTransformations:    []scopeTransformation{},
		logTransformations:      []logTransformation{},
	}

	// Iterate over context statements and parse them based on their context type
	for _, cs := range contextStatements {
		switch cs.Context {
		case "resource":
			// Parse conditions and statements for resource context
			conditions, err := resourceParser.ParseConditions(cs.Conditions)
			if err != nil {
				lp.logger.Error("Error parsing resource conditions", zap.Error(err))
				continue
			}
			statements, err := resourceParser.ParseStatements(cs.Statements)
			if err != nil {
				lp.logger.Error("Error parsing resource statements", zap.Error(err))
				continue
			}

			// Append parsed resource transformations
			transformations.resourceTransformations = append(transformations.resourceTransformations, resourceTransformation{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "scope":
			// Parse conditions and statements for scope context
			conditions, err := scopeParser.ParseConditions(cs.Conditions)
			if err != nil {
				lp.logger.Error("Error parsing scope conditions", zap.Error(err))
				continue
			}
			statements, err := scopeParser.ParseStatements(cs.Statements)
			if err != nil {
				lp.logger.Error("Error parsing scope statements", zap.Error(err))
				continue
			}

			// Append parsed scope transformations
			transformations.scopeTransformations = append(transformations.scopeTransformations, scopeTransformation{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "log":
			// Parse conditions and statements for log context
			conditions, err := logParser.ParseConditions(cs.Conditions)
			if err != nil {
				lp.logger.Error("Error parsing log conditions", zap.Error(err))
				continue
			}
			statements, err := logParser.ParseStatements(cs.Statements)
			if err != nil {
				lp.logger.Error("Error parsing log statements", zap.Error(err))
				continue
			}

			// Append parsed log transformations
			transformations.logTransformations = append(transformations.logTransformations, logTransformation{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		default:
			// Handle unknown context types
			println("Unknown context: ", cs.Context)
		}
	}

	return transformations
}

func newLogsProcessor(set processor.Settings, cfg *Config) (*logProcessor, error) {
	lp := &logProcessor{
		logger:  set.Logger,
		podName: os.Getenv("POD_NAME"),
	}
	lp.transformations = lp.toTransformations(cfg.LogsConfig.Transforms)

	set.Logger.Info("Decorator processor configured")

	lp.finger = fingerprinter.NewFingerprinter()

	return lp, nil
}

func (lp *logProcessor) evaluateResourceTransformations(rl plog.ResourceLogs) {
	transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
	for _, transformation := range lp.transformations.resourceTransformations {
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

func (lp *logProcessor) evaluateScopeTransformations(sl plog.ScopeLogs, rl plog.ResourceLogs) {
	transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
	for _, transformation := range lp.transformations.scopeTransformations {
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

func (lp *logProcessor) evaluateLogTransformations(log plog.LogRecord, sl plog.ScopeLogs, rl plog.ResourceLogs) {
	transformCtx := ottllog.NewTransformContext(log, sl.Scope(), rl.Resource(), sl, rl)
	for _, transformation := range lp.transformations.logTransformations {
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

func (lp *logProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	environment := translate.EnvironmentFromEnv()
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		// Evaluate resource transformations
		lp.evaluateResourceTransformations(rl)

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			// Evaluate scope transformations
			lp.evaluateScopeTransformations(sl, rl)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level, err := lp.finger.Fingerprint(log.Body().AsString())
				if err != nil {
					lp.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}
				// Evaluate log scope transformations
				lp.evaluateLogTransformations(log, sl, rl)

				log.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				log.Attributes().PutStr(translate.CardinalFieldLevel, level)
				log.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, lp.podName)
				log.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				log.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
			}
		}
	}

	return ld, nil
}

func (lp *logProcessor) Shutdown(_ context.Context) error {
	return nil
}
