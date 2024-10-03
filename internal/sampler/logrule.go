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

package sampler

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logRule struct {
	id       string
	ruleType LogRuleType
	sampler  Sampler
	config   LogSamplingConfigV1

	resourceCondition *ottl.Condition[ottlresource.TransformContext]
	scopeCondition    *ottl.Condition[ottlscope.TransformContext]
	logCondition      *ottl.Condition[ottllog.TransformContext]
}

func (lr *logRule) parseConditions() {
	resourceParser, _ := ottlresource.NewParser(ottlfuncs.StandardFuncs[ottlresource.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	scopeParser, _ := ottlscope.NewParser(ottlfuncs.StandardFuncs[ottlscope.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	logParser, _ := ottllog.NewParser(ottlfuncs.StandardFuncs[ottllog.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	if lr.config.Filter != nil {
		for _, filter := range lr.config.Filter {
			switch filter.ContextId {
			case "resource":
				lr.resourceCondition, _ = resourceParser.ParseCondition(filter.Condition)

			case "scope":
				lr.scopeCondition, _ = scopeParser.ParseCondition(filter.Condition)

			case "log":
				lr.logCondition, _ = logParser.ParseCondition(filter.Condition)
			}
		}
	}
}

func (lr *logRule) evaluate(rl plog.ResourceLogs, sl plog.ScopeLogs, ll plog.LogRecord) bool {
	if lr.resourceCondition != nil {
		transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		eval, err := lr.resourceCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}

	if lr.scopeCondition != nil {
		transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
		eval, err := lr.scopeCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}

	if lr.logCondition != nil {
		transformCtx := ottllog.NewTransformContext(ll, sl.Scope(), rl.Resource(), sl, rl)
		eval, err := lr.logCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}
	return true
}

func newLogRule(c LogSamplingConfigV1) *logRule {
	// Create the logRule instance
	r := &logRule{
		id:       c.Id,
		ruleType: logRuletypeToInt(c.RuleType),
		config:   c,
	}

	// Call parseConditions to initialize the conditions
	r.parseConditions()

	return r
}
