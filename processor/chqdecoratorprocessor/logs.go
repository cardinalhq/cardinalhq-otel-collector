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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func (c *chqDecorator) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	c.Lock()
	defer c.Unlock()

	environment := translate.EnvironmentFromEnv()
	transformations := c.logTransformations

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		serviceName := getServiceName(rl.Resource().Attributes())
		// Evaluate resource transformations
		resourceCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		transformations.ExecuteResourceTransforms(resourceCtx, "", pcommon.Slice{})

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			// Evaluate scope transformations
			scopeCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), rl)
			transformations.ExecuteScopeTransforms(scopeCtx, "", pcommon.Slice{})

			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level, err := c.logFingerprinter.Fingerprint(log.Body().AsString())
				if err != nil {
					c.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}

				// Evaluate log scope transformations
				logCtx := ottllog.NewTransformContext(log, sl.Scope(), rl.Resource(), sl, rl)
				transformations.ExecuteLogTransforms(logCtx, "", pcommon.Slice{})

				// Evaluate Log sampling Rules
				c.evaluateLogSamplingRules(serviceName, fingerprint, rl, sl, log)

				log.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				log.Attributes().PutStr(translate.CardinalFieldLevel, level)
				log.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, c.podName)
				log.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				log.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
			}
		}
	}

	return ld, nil
}
func (c *chqDecorator) evaluateLogSamplingRules(serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) {
	fingerprintString := fmt.Sprintf("%d", fingerprint)
	ruleMatches := c.logSampler.SampleLogs(serviceName, fingerprintString, rl, sl, lr)
	attributes := lr.Attributes()

	if len(ruleMatches) > 0 {
		for _, ruleMatch := range ruleMatches {
			c.appendToSlice(attributes, translate.CardinalFieldDropForVendor, ruleMatch.VendorId)
			c.appendToSlice(attributes, translate.CardinalFieldRulesMatched, ruleMatch.RuleId)
		}
	}
}

func (c *chqDecorator) updateLogsSampling(sc sampler.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating logs transformation config...")
	c.logSampler.UpdateConfig(sc.Logs.SamplingRules, c.telemetrySettings)
	for _, decorator := range sc.Logs.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing log transformation", zap.Error(err))
		} else {
			c.logTransformations = ottl.MergeWith(c.logTransformations, transformations)
		}
	}
}
