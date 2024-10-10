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
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

func LogFunctions() map[string]ottl.Factory[ottllog.TransformContext] {
	functions := ottlfuncs.StandardFuncs[ottllog.TransformContext]()

	datapointFunctions := ottl.CreateFactoryMap[ottllog.TransformContext](
		newLogSamplerFactory(),
	)

	for k, v := range datapointFunctions {
		functions[k] = v
	}

	return functions
}

type logSamplerArguments struct {
	Name ottl.Optional[ottl.StringGetter[ottllog.TransformContext]]
}

func newLogSamplerFactory() ottl.Factory[ottllog.TransformContext] {
	return ottl.NewFactory("sample", &logSamplerArguments{}, createSampleLogFunction)
}

func createSampleLogFunction(_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[ottllog.TransformContext], error) {
	args, ok := oArgs.(*logSamplerArguments)
	if !ok {
		return nil, fmt.Errorf("createSampleLogFunction args must be of type *logSamplerArguments")
	}

	return sampleLog(args.Name)
}

func sampleLog(name ottl.Optional[ottl.StringGetter[ottllog.TransformContext]]) (ottl.ExprFunc[ottllog.TransformContext], error) {
	return func(ctx context.Context, tCtx ottllog.TransformContext) (any, error) {

		//cur := tCtx.GetLogRecord()

		return nil, nil
	}, nil
}
