// Copyright 2024-2025 CardinalHQ, Inc
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

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.uber.org/zap"
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

func (p *pitbull) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		cid := orgIDFromResource(rl.Resource().Attributes())
		transformations, transformationsFound := p.logTransformations.Load(cid)
		luc, lucFound := p.logsLookupConfigs.Load(cid)
		if !transformationsFound && !lucFound {
			return false
		}
		attrSet := p.attributesFor(cid)
		transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl) //nolint:staticcheck // deprecated but new API has different signature
		if transformations != nil {
			transformations.ExecuteResourceTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
			if _, found := rl.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
				return true
			}
		}

		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), rl) //nolint:staticcheck // deprecated but new API has different signature
			if transformations != nil {
				transformations.ExecuteScopeTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
				if _, found := sl.Scope().Attributes().Get(translate.CardinalFieldDropMarker); found {
					return true
				}
			}

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				transformCtx := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl) //nolint:staticcheck // deprecated but new API has different signature
				if luc != nil {
					for _, lookupConfig := range *luc {
						lookupConfig.ExecuteLogsRules(context.Background(), transformCtx, lr)
					}
				}
				if transformations == nil {
					return false
				}
				transformations.ExecuteLogTransforms(p.logger, attrSet, p.ottlTelemetry, transformCtx)
				_, dropMe := lr.Attributes().Get(translate.CardinalFieldDropMarker)
				return dropMe
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

func (p *pitbull) updateLogConfigForTenant(cid string, pbc *ottl.PitbullProcessorConfig) {
	if pbc == nil {
		p.shutdownLogsForTenant(cid)
		return
	}
	p.logger.Info("Updating log transformations", zap.String("organizationID", cid), zap.Int("logTransformations", len(pbc.LogStatements)), zap.Int("logLookupConfigs", len(pbc.LogLookupConfigs)))

	newTransformations := ottl.NewTransformations()
	transformations, err := ottl.ParseTransformations(p.logger, pbc.LogStatements)
	if err != nil {
		p.logger.Error("Error parsing log transformation", zap.Error(err))
	} else {
		newTransformations = ottl.MergeWith(newTransformations, transformations)
	}

	if old, found := p.logTransformations.Replace(cid, newTransformations); found {
		old.Stop()
	}

	for _, lookupConfig := range pbc.LogLookupConfigs {
		lookupConfig.Init(p.logger)
	}
	p.logsLookupConfigs.Store(cid, &pbc.LogLookupConfigs)
}
