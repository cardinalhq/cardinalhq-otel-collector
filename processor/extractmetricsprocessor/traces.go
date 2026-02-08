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
	"time"

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/observiq/bindplane-otel-collector/receiver/routereceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.uber.org/zap"
)

func (p *extractor) ConsumeTraces(ctx context.Context, pt ptrace.Traces) (ptrace.Traces, error) {
	p.updateSketchCache(ctx, pt)
	return pt, nil
}

func (e *extractor) sendMetrics(ctx context.Context, route string, metrics pmetric.Metrics) {
	e.logger.Debug("Sending span metrics", zap.String("route", route), zap.Int("num_metrics", metrics.DataPointCount()))

	if err := routereceiver.RouteMetrics(ctx, route, metrics); err != nil {
		e.logger.Error("Failed to send metrics", zap.Error(err))
	}
}

type Emittable struct {
	MetricName string
	MetricType string            // e.g. "count", "gauge", "sum"
	Tags       map[string]string // flat map, includes resource + dp attrs
	IntervalMs int64             // unix ms timestamp
	Value      *float64
}

// splitResourceAndAttrs divides a flat tags map into resource-level keys and dp attributes.
func (p *extractor) splitResourceAndAttrs(tags map[string]string) (resourceMap pcommon.Map, attrMap pcommon.Map) {
	serviceNameKey := p.toServiceNameKey()
	clusterNameKey := p.toClusterNameKey()
	namespaceNameKey := p.toNamespaceNameKey()

	resourceMap = pcommon.NewMap()
	attrMap = pcommon.NewMap()

	if v, ok := tags[serviceNameKey]; ok {
		resourceMap.PutStr(string(semconv.ServiceNameKey), v)
	}
	if v, ok := tags[clusterNameKey]; ok {
		resourceMap.PutStr(string(semconv.K8SClusterNameKey), v)
	}
	if v, ok := tags[namespaceNameKey]; ok {
		resourceMap.PutStr(string(semconv.K8SNamespaceNameKey), v)
	}

	for k, v := range tags {
		if k == serviceNameKey || k == clusterNameKey || k == namespaceNameKey {
			continue
		}
		attrMap.PutStr(k, v)
	}
	return
}

// emitMetrics builds a pmetric.Metrics payload from emittables and sends it.
func (p *extractor) emitMetrics(ctx context.Context, route string, rows []Emittable) {
	if len(rows) == 0 {
		return
	}
	mb := signalbuilder.NewMetricsBuilder()

	for i := range rows {
		row := rows[i]
		resMap, dpAttrs := p.splitResourceAndAttrs(row.Tags)
		rmb := mb.Resource(resMap)
		smb := rmb.Scope(pcommon.NewMap())

		switch row.MetricType {
		case "count":
			mdb, err := smb.Metric(row.MetricName, "count", pmetric.MetricTypeSum)
			if err != nil {
				p.logger.Error("Failed to create metric", zap.String("metric", row.MetricName), zap.Error(err))
				continue
			}
			// Preserve existing behavior that relies on helper Datapoint(attrs, ts)
			dp, _, isNew := mdb.Datapoint(dpAttrs, pcommon.NewTimestampFromTime(time.UnixMilli(row.IntervalMs)))
			if isNew {
				dp.SetDoubleValue(*row.Value)
			} else {
				dp.SetDoubleValue(dp.DoubleValue() + *row.Value)
			}

		case "gauge":
			mdb, err := smb.Metric(row.MetricName, "gauge", pmetric.MetricTypeGauge)
			if err != nil {
				p.logger.Error("Failed to create metric", zap.String("metric", row.MetricName), zap.Error(err))
				continue
			}
			dp, _, isNew := mdb.Datapoint(dpAttrs, pcommon.NewTimestampFromTime(time.UnixMilli(row.IntervalMs)))
			if isNew {
				dp.SetDoubleValue(*row.Value)
			} else {
				if dp.DoubleValue() < *row.Value {
					dp.SetDoubleValue(*row.Value)
				}
			}

		default:
			// Fallback to gauge if unknown
			mdb, err := smb.Metric(row.MetricName, row.MetricType, pmetric.MetricTypeGauge)
			if err != nil {
				p.logger.Error("Failed to create metric", zap.String("metric", row.MetricName), zap.Error(err))
				continue
			}
			dp, _, isNew := mdb.Datapoint(dpAttrs, pcommon.NewTimestampFromTime(time.UnixMilli(row.IntervalMs)))
			if isNew {
				dp.SetDoubleValue(*row.Value)
			} else {
				if dp.DoubleValue() < *row.Value {
					dp.SetDoubleValue(*row.Value)
				}
			}
		}
	}

	metrics := mb.Build()
	p.sendMetrics(ctx, route, metrics)
}

func (p *extractor) spanSketchesToEmittables(list *chqpb.SpanSketchList) []Emittable {
	if list == nil || len(list.Sketches) == 0 {
		return nil
	}
	out := make([]Emittable, 0, len(list.Sketches))
	for _, s := range list.Sketches {
		totalCount := float64(s.TotalCount)
		intervalMs := s.Interval * 1000
		out = append(out, Emittable{
			MetricName: s.MetricName + ".total",
			MetricType: "count",
			Tags:       s.Tags,
			IntervalMs: intervalMs,
			Value:      &totalCount,
		})
		errorCount := float64(s.ErrorCount)
		out = append(out, Emittable{
			MetricName: s.MetricName + ".errors",
			MetricType: "count",
			Tags:       s.Tags,
			IntervalMs: intervalMs,
			Value:      &errorCount,
		})
		if s.Sketch != nil {
			decodedSketch, err := chqpb.DecodeSketch(s.Sketch)
			if err != nil {
				p.logger.Error("Failed to decode sketch", zap.Error(err))
				continue
			}
			p50Value, err := decodedSketch.GetValueAtQuantile(0.5)
			if err != nil {
				p.logger.Error("Failed to get p50 from sketch", zap.Error(err))
			} else {
				out = append(out, Emittable{
					MetricName: s.MetricName + ".p50",
					MetricType: "gauge",
					Tags:       s.Tags,
					IntervalMs: intervalMs,
					Value:      &p50Value,
				})
			}
			p95Value, err := decodedSketch.GetValueAtQuantile(0.95)
			if err != nil {
				p.logger.Error("Failed to get p95 from sketch", zap.Error(err))
			} else {
				out = append(out, Emittable{
					MetricName: s.MetricName + ".p95",
					MetricType: "gauge",
					Tags:       s.Tags,
					IntervalMs: intervalMs,
					Value:      &p95Value,
				})
			}
			p99Value, err := decodedSketch.GetValueAtQuantile(0.99)
			if err != nil {
				p.logger.Error("Failed to get p99 from sketch", zap.Error(err))
			} else {
				out = append(out, Emittable{
					MetricName: s.MetricName + ".p99",
					MetricType: "gauge",
					Tags:       s.Tags,
					IntervalMs: intervalMs,
					Value:      &p99Value,
				})
			}
		}
	}
	return out
}

type ResourcesKey struct {
	OrganizationID string
	ServiceName    string
	ClusterName    string
	NamespaceName  string
}

func (p *extractor) updateSketchCache(ctx context.Context, pl ptrace.Traces) {
	resourceSpans := pl.ResourceSpans()
	for i := range resourceSpans.Len() {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		cid := OrgIdFromResource(resource.Attributes())

		spanExtractors, ok := p.spanExtractors.Load(cid)
		if !ok {
			continue
		}

		spanAggregateSketchCache, sok := p.spanAggregateSketchCaches.Load(cid)
		if !sok {
			p.logger.Info("Creating new span aggregate sketch cache", zap.String("cid", cid))
			spanAggregateSketchCache = chqpb.NewSpanSketchCache(
				10*time.Second,
				cid,
				20,
				func(list *chqpb.SpanSketchList) error {
					rows := p.spanSketchesToEmittables(list)
					p.emitMetrics(ctx, p.config.Route, rows)
					return nil
				})
			p.spanAggregateSketchCaches.Store(cid, spanAggregateSketchCache)
		}

		spanLineSketchCache, okLine := p.spanLineSketchCaches.Load(cid)
		if !okLine {
			p.logger.Info("Creating new span line sketch cache", zap.String("cid", cid))
			spanLineSketchCache = chqpb.NewSpanSketchCache(
				10*time.Second,
				cid,
				20,
				func(list *chqpb.SpanSketchList) error {
					rows := p.spanSketchesToEmittables(list)
					p.emitMetrics(ctx, p.config.Route, rows)
					return nil
				},
			)
			p.spanLineSketchCaches.Store(cid, spanLineSketchCache)
		}

		// ----- Iterate spans and apply extraction rules -----
		scopeSpans := resourceSpans.At(i).ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			spans := scopeSpan.Spans()

			for k := 0; k < spans.Len(); k++ {
				lr := spans.At(k)
				tc := ottlspan.NewTransformContext(lr, scopeSpan.Scope(), resourceSpan.Resource(), scopeSpan, resourceSpan) //nolint:staticcheck // deprecated but new API has different signature

				for _, lex := range spanExtractors {
					attrset := attribute.NewSet(
						attribute.String("processor", p.id.String()),
						attribute.String("signal", p.ttype),
						attribute.String("ruleId", lex.RuleID),
						attribute.String("metricName", lex.MetricName),
						attribute.String("metricType", lex.MetricType),
						attribute.String("organization_id", cid),
					)

					matches, err := lex.EvalSpanConditions(ctx, tc)
					if err != nil {
						p.logger.Error("Failed when executing ottl match statement.", zap.Error(err))
						telemetry.CounterAdd(p.ruleErrors, 1, metric.WithAttributeSet(attrset))
						continue
					}
					if !matches {
						continue
					}

					telemetry.CounterAdd(p.rulesEvaluated, 1, metric.WithAttributeSet(attrset))

					aggregateTags := p.withServiceClusterNamespace(resource, lex.ExtractAggregateAttributes(ctx, tc))
					spanAggregateSketchCache.Update(lex.MetricName, lex.MetricType, aggregateTags, lr, resource, 0, 0)

					// Otherwise, do span sketches (count-style) at the line level.
					if len(lex.LineDimensions) > 0 {
						mapAttrsByTagFamilyId := lex.ExtractLineAttributes(ctx, tc)
						for tagFamilyId, mapAttrs := range mapAttrsByTagFamilyId {
							lineTags := p.withServiceClusterNamespace(resource, mapAttrs)
							spanLineSketchCache.Update(
								lex.MetricName,
								lex.MetricType,
								lineTags,
								lr,
								resource,
								0,
								tagFamilyId,
							)
						}
					}
				}
			}
		}
	}
}
