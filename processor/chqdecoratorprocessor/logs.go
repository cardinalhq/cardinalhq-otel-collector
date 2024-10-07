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
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		serviceName := getServiceName(rl.Resource().Attributes())

		// Evaluate resource transformations
		c.logTransformations.ExecuteResourceLogTransforms(rl)

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			// Evaluate scope transformations
			c.logTransformations.ExecuteScopeLogTransforms(sl, rl)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level, err := c.logFingerprinter.Fingerprint(log.Body().AsString())
				if err != nil {
					c.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}

				// Evaluate log scope transformations
				c.logTransformations.ExecuteLogTransforms(log, sl, rl)

				// Evaluate if we should drop this log
				shouldDrop := c.shouldDropLog(serviceName, fingerprint, rl, sl, log)

				log.Attributes().PutBool(translate.CardinalFieldDrop, shouldDrop)
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

func (c *chqDecorator) shouldDropLog(serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) bool {
	fingerprintString := fmt.Sprintf("%d", fingerprint)
	return c.logSampler.SampleLogs(serviceName, fingerprintString, rl, sl, lr) != ""
}

func (c *chqDecorator) updateLogsSampling(sc sampler.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating log sampling config", zap.String("vendor", c.vendor))
	c.logSampler.UpdateConfig(sc.Logs.Sampling, c.vendor, c.telemetrySettings)
	// ok to ignore the parse error here, because we expect the config to be valid because it got validated
	// before it was saved by the UI.
	transformations, _ := ottl.ParseTransformations(sc.Logs.Transformations, c.logger)
	c.logTransformations = transformations
}
