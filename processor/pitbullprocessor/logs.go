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

package pitbullprocessor

import (
	"context"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func getFingerprint(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}

func (e *pitbull) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	e.Lock()
	defer e.Unlock()

	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	environment := translate.EnvironmentFromEnv()
	emptySlice := pcommon.NewSlice()

	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		e.logTransformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
		if _, found := rl.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
			return true
		}

		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), rl)
			e.logTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
			if _, found := sl.Scope().Attributes().Get(translate.CardinalFieldDropMarker); found {
				return true
			}

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				fingerprint, level, err := e.logFingerprinter.Fingerprint(lr.Body().AsString())
				if err != nil {
					e.logger.Debug("Error fingerprinting log", zap.Error(err))
					return true
				}

				lr.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				lr.Attributes().PutStr(translate.CardinalFieldLevel, level)
				lr.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, e.podName)
				lr.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				lr.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())

				transformCtx := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
				e.logTransformations.ExecuteLogTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.config.Vendor), emptySlice)
				_, found := lr.Attributes().Get(translate.CardinalFieldDropMarker)
				return found
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	if ld.ResourceLogs().Len() == 0 {
		return ld, processorhelper.ErrSkipProcessingData
	}
	return ld, nil
}

func removeAllCardinalFields(attr pcommon.Map) {
	attr.RemoveIf(func(k string, _ pcommon.Value) bool {
		return strings.HasPrefix(k, translate.CardinalFieldPrefix)
	})
}

func (c *pitbull) updateLogTransformations(sc ottl.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating log transformations", zap.Int("num_decorators", len(sc.Logs.Decorators)))
	newTransformations := ottl.NewTransformations(c.logger)

	c.logger.Info("json log config", zap.Any("logDecorators", sc.Logs))

	for _, decorator := range sc.Logs.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing log transformation", zap.Error(err))
		} else {
			newTransformations = ottl.MergeWith(newTransformations, transformations)
		}
	}

	oldTransformation := c.logTransformations
	c.logTransformations = newTransformations
	oldTransformation.Stop()
}
