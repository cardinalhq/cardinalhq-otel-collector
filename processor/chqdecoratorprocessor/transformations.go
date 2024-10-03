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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
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

type spanTransformation struct {
	context    ContextID
	conditions []*ottl.Condition[ottlspan.TransformContext]
	statements []*ottl.Statement[ottlspan.TransformContext]
}

type transformations struct {
	resourceTransformations []resourceTransformation
	scopeTransformations    []scopeTransformation
	logTransformations      []logTransformation
	spanTransformations     []spanTransformation
}

func toTransformations(contextStatements []ContextStatement, logger *zap.Logger) transformations {
	// Set up parsers for each context type (resource, scope, log)
	resourceParser, _ := ottlresource.NewParser(ottlfuncs.StandardFuncs[ottlresource.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	scopeParser, _ := ottlscope.NewParser(ottlfuncs.StandardFuncs[ottlscope.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	logParser, _ := ottllog.NewParser(ottlfuncs.StandardFuncs[ottllog.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})
	spanParser, _ := ottlspan.NewParser(ottlfuncs.StandardFuncs[ottlspan.TransformContext](), component.TelemetrySettings{Logger: zap.NewNop()})

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
				logger.Error("Error parsing resource conditions", zap.Error(err))
				continue
			}
			statements, err := resourceParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing resource statements", zap.Error(err))
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
				logger.Error("Error parsing scope conditions", zap.Error(err))
				continue
			}
			statements, err := scopeParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing scope statements", zap.Error(err))
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
				logger.Error("Error parsing log conditions", zap.Error(err))
				continue
			}
			statements, err := logParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing log statements", zap.Error(err))
				continue
			}

			// Append parsed log transformations
			transformations.logTransformations = append(transformations.logTransformations, logTransformation{
				context:    cs.Context,
				conditions: conditions,
				statements: statements,
			})

		case "span":
			// Parse conditions and statements for log context
			conditions, err := spanParser.ParseConditions(cs.Conditions)
			if err != nil {
				logger.Error("Error parsing log conditions", zap.Error(err))
				continue
			}
			statements, err := spanParser.ParseStatements(cs.Statements)
			if err != nil {
				logger.Error("Error parsing log statements", zap.Error(err))
				continue
			}

			// Append parsed span transformations
			transformations.spanTransformations = append(transformations.spanTransformations, spanTransformation{
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
