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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
	"os"
)

type logProcessor struct {
	logger  *zap.Logger
	finger  fingerprinter.Fingerprinter
	podName string

	transformations transformations
}

func newLogsProcessor(set processor.Settings, cfg *Config) (*logProcessor, error) {
	lp := &logProcessor{
		logger:  set.Logger,
		podName: os.Getenv("POD_NAME"),
	}
	lp.transformations = toTransformations(cfg.LogsConfig.Transforms, lp.logger)

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
