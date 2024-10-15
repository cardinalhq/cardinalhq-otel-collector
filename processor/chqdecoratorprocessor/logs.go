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

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

func (c *chqDecorator) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	c.Lock()
	defer c.Unlock()

	environment := translate.EnvironmentFromEnv()
	transformations := c.logTransformations
	emptySlice := pcommon.NewSlice()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		// Evaluate resource transformations
		c.ottlProcessed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("phase", "pre-resource")))
		resourceCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		transformations.ExecuteResourceTransforms(c.ottlProcessed, resourceCtx, "", emptySlice)

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			// Evaluate scope transformations
			c.ottlProcessed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("phase", "pre-scope")))
			scopeCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), rl)
			transformations.ExecuteScopeTransforms(c.ottlProcessed, scopeCtx, "", emptySlice)

			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level, err := c.logFingerprinter.Fingerprint(log.Body().AsString())
				if err != nil {
					c.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}

				log.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				log.Attributes().PutStr(translate.CardinalFieldLevel, level)
				log.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, c.podName)
				log.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				log.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())

				// Evaluate log scope transformations
				c.ottlProcessed.Add(context.Background(), 1, metric.WithAttributes(attribute.String("phase", "pre-log-record")))
				logCtx := ottllog.NewTransformContext(log, sl.Scope(), rl.Resource(), sl, rl)
				transformations.ExecuteLogTransforms(c.ottlProcessed, logCtx, "", emptySlice)
			}
		}
	}

	return ld, nil
}

func (c *chqDecorator) updateLogsSampling(sc ottl.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating log transformations", zap.Int("num_decorators", len(sc.Logs.Decorators)))
	newTransformations := ottl.NewTransformations(c.logger)

	for _, decorator := range sc.Logs.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing log transformation", zap.Error(err))
		} else {
			newTransformations = ottl.MergeWith(newTransformations, transformations)
		}
	}

	oldTransformation := c.traceTransformations
	c.traceTransformations = newTransformations
	oldTransformation.Stop()
}
