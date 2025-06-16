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

package extractmetricsprocessor

import (
	"context"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"time"

	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/ottl"
)

func (p *extractor) ConsumeLogs(ctx context.Context, pl plog.Logs) (plog.Logs, error) {
	p.extract(ctx, pl)
	return pl, nil
}

func (p *extractor) extract(ctx context.Context, pl plog.Logs) {
	resourceLogs := pl.ResourceLogs()
	for i := range resourceLogs.Len() {
		resourceLog := resourceLogs.At(i)
		resource := resourceLog.Resource()
		cid := OrgIdFromResource(resource.Attributes())
		logExtractors, ok := p.logExtractors.Load(cid)
		if !ok {
			continue
		}

		aggregateSketchCache, sok := p.logsAggregateSketchCaches.Load(cid)
		if !sok {
			p.logger.Info("Creating new logs aggregate sketch cache", zap.String("cid", cid))
			aggregateSketchCache = chqpb.NewGenericSketchCache(1*time.Minute, cid, "logs", 20, func(list *chqpb.GenericSketchList) error {
				send := p.sendProto("/api/v1/metricSketches", list)
				return send()
			})
			p.logsAggregateSketchCaches.Store(cid, aggregateSketchCache)
		}

		lineSketchCache, sok := p.logsLineSketchCaches.Load(cid)
		if !sok {
			p.logger.Info("Creating new logs line sketch cache", zap.String("cid", cid))
			aggregateSketchCache = chqpb.NewGenericSketchCache(1*time.Minute, cid, "logs", 20, func(list *chqpb.GenericSketchList) error {
				send := p.sendProto("/api/v1/metricSketches", list)
				return send()
			})
			p.logsLineSketchCaches.Store(cid, lineSketchCache)
		}

		for j := range resourceLogs.At(i).ScopeLogs().Len() {
			scopeLog := resourceLogs.At(i).ScopeLogs().At(j)
			for k := range resourceLogs.At(i).ScopeLogs().At(j).LogRecords().Len() {
				lr := resourceLogs.At(i).ScopeLogs().At(j).LogRecords().At(k)
				tc := ottllog.NewTransformContext(lr, scopeLog.Scope(), resourceLog.Resource(), scopeLog, resourceLog)

				for _, lex := range logExtractors {
					attrset := attribute.NewSet(
						attribute.String("processor", p.id.String()),
						attribute.String("signal", p.ttype),
						attribute.String("ruleId", lex.RuleID),
						attribute.String("metricName", lex.MetricName),
						attribute.String("metricType", lex.MetricType),
						attribute.String("organization_id", cid),
					)

					matches, err := lex.EvalLogConditions(ctx, tc)
					if err != nil {
						p.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}
					if !matches {
						continue
					}

					val, err := p.extractLogValue(ctx, tc, lex)
					if err != nil {
						p.logger.Error("Failed when extracting value.", zap.Error(err))
						attrset := attribute.NewSet(attribute.String("ruleId", lex.RuleID),
							attribute.String("metricName", lex.MetricName),
							attribute.String("metricType", lex.MetricType),
							attribute.String("stage", "metricValueExtraction"),
							attribute.String("error", err.Error()),
							attribute.String("organization_id", cid),
						)

						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}
					telemetry.CounterAdd(p.rulesEvaluated, 1, metric.WithAttributeSet(attrset))

					aggregateAttrs := lex.ExtractAggregateAttributes(ctx, tc)
					tags := p.withServiceClusterNamespace(resource, aggregateAttrs)
					parentTID := aggregateSketchCache.Update(lex.MetricName, lex.MetricType, lex.Direction, tags, 0, 0, val, lr.ObservedTimestamp().AsTime())

					if len(lex.LineDimensions) > 0 {
						mapAttrsByTagFamilyId := lex.ExtractLineAttributes(ctx, tc)
						for tagFamilyId, mapAttrs := range mapAttrsByTagFamilyId {
							metricTags := p.withServiceClusterNamespace(resource, mapAttrs)
							lineSketchCache.Update(lex.MetricName, lex.MetricType, lex.Direction, metricTags, parentTID, tagFamilyId, val, lr.ObservedTimestamp().AsTime())
						}
					}
				}
			}
		}
	}
}

func (p *extractor) withServiceClusterNamespace(resource pcommon.Resource, mapAttrs map[string]any) map[string]string {
	serviceName, serviceNameFound := resource.Attributes().Get(string(semconv.ServiceNameKey))
	clusterName, clusterNameFound := resource.Attributes().Get(string(semconv.K8SClusterNameKey))
	namespaceName, namespaceNameFound := resource.Attributes().Get(string(semconv.K8SNamespaceNameKey))

	tags := make(map[string]string, len(mapAttrs))
	for k, v := range mapAttrs {
		if str, ok := v.(string); ok {
			tags[k] = str
		}
	}
	if serviceNameFound {
		tags[fmt.Sprintf("resource.%s", string(semconv.ServiceNameKey))] = serviceName.AsString()
	}
	if clusterNameFound {
		tags[fmt.Sprintf("resource.%s", string(semconv.K8SClusterNameKey))] = clusterName.AsString()
	}
	if namespaceNameFound {
		tags[fmt.Sprintf("resource.%s", string(semconv.K8SNamespaceNameKey))] = namespaceName.AsString()
	}
	return tags
}

func (p *extractor) extractLogValue(ctx context.Context, tc ottllog.TransformContext, e *ottl.LogExtractor) (float64, error) {
	if e.MetricValue != nil {
		val, _, err := e.MetricValue.Execute(ctx, tc)
		if err != nil {
			return 0, err
		}
		return convertAnyToFloat(val)
	}
	return 1, nil
}
