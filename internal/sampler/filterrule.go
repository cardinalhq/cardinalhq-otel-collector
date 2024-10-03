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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type filterRule struct {
	id       string
	ruleType EventSamplingRuleType
	sampler  Sampler
	config   EventSamplingConfigV1

	resourceCondition *ottl.Condition[ottlresource.TransformContext]
	scopeCondition    *ottl.Condition[ottlscope.TransformContext]
	logCondition      *ottl.Condition[ottllog.TransformContext]
	spanCondition     *ottl.Condition[ottlspan.TransformContext]
}

func (fr *filterRule) parseConditions() {
	resourceParser, _ := ottlresource.NewParser(ottlfuncs.StandardFuncs[ottlresource.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	scopeParser, _ := ottlscope.NewParser(ottlfuncs.StandardFuncs[ottlscope.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	logParser, _ := ottllog.NewParser(ottlfuncs.StandardFuncs[ottllog.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	spanParser, _ := ottlspan.NewParser(ottlfuncs.StandardFuncs[ottlspan.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()})

	if fr.config.Filter != nil {
		for _, filter := range fr.config.Filter {
			switch filter.ContextId {
			case "resource":
				fr.resourceCondition, _ = resourceParser.ParseCondition(filter.Condition)

			case "scope":
				fr.scopeCondition, _ = scopeParser.ParseCondition(filter.Condition)

			case "log":
				fr.logCondition, _ = logParser.ParseCondition(filter.Condition)

			case "span":
				fr.spanCondition, _ = spanParser.ParseCondition(filter.Condition)
			}
		}
	}
}

func (fr *filterRule) evaluateLog(rl plog.ResourceLogs, sl plog.ScopeLogs, ll plog.LogRecord) bool {
	if fr.resourceCondition != nil {
		transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		eval, err := fr.resourceCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}

	if fr.scopeCondition != nil {
		transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
		eval, err := fr.scopeCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}

	if fr.logCondition != nil {
		transformCtx := ottllog.NewTransformContext(ll, sl.Scope(), rl.Resource(), sl, rl)
		eval, err := fr.logCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}
	return true
}

func (fr *filterRule) evaluateSpan(rl ptrace.ResourceSpans, sl ptrace.ScopeSpans, ll ptrace.Span) bool {
	if fr.resourceCondition != nil {
		transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		eval, err := fr.resourceCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}

	if fr.scopeCondition != nil {
		transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), sl)
		eval, err := fr.scopeCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}

	if fr.spanCondition != nil {
		transformCtx := ottlspan.NewTransformContext(ll, sl.Scope(), rl.Resource(), sl, rl)
		eval, err := fr.spanCondition.Eval(context.Background(), transformCtx)
		if err != nil || !eval {
			return false
		}
	}
	return true
}

func newFilterRule(c EventSamplingConfigV1) *filterRule {
	// Create the logRule instance
	r := &filterRule{
		id:       c.Id,
		ruleType: samplingRuleTypeToInt(c.RuleType),
		config:   c,
	}

	// Call parseConditions to initialize the conditions
	r.parseConditions()

	return r
}
