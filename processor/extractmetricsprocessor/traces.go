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
	"bytes"
	"context"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func (p *extractor) ConsumeTraces(ctx context.Context, pt ptrace.Traces) (ptrace.Traces, error) {
	p.updateSketchCache(ctx, pt)
	return pt, nil
}

func (p *extractor) sendSketches(list *chqpb.SpanSketchList) error {
	if len(list.Sketches) > 0 {
		p.logger.Info("Sending span stats", zap.Int("sketches", len(list.Sketches)))
		b, err := proto.Marshal(list)
		if err != nil {
			return err
		}
		endpoint := p.config.Endpoint + "/api/v1/spanSketches"
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-protobuf")

		resp, err := p.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			p.logger.Error("Failed to send span stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
	return nil
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

		sketchCache, sok := p.spanSketchCaches.Load(cid)
		if !sok {
			p.logger.Info("Creating new span sketch cache", zap.String("cid", cid))
			sketchCache = chqpb.NewSpanSketchCache(5*time.Minute, cid, p.sendSketches)
			p.spanSketchCaches.Store(cid, sketchCache)
		}

		for j := range resourceSpans.At(i).ScopeSpans().Len() {
			scopeSpan := resourceSpans.At(i).ScopeSpans().At(j)
			for k := range resourceSpans.At(i).ScopeSpans().At(j).Spans().Len() {
				lr := resourceSpans.At(i).ScopeSpans().At(j).Spans().At(k)
				tc := ottlspan.NewTransformContext(lr, scopeSpan.Scope(), resourceSpan.Resource(), scopeSpan, resourceSpan)

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

					if len(lex.LineDimensions) > 0 {
						mapAttrs := lex.ExtractLineAttributes(ctx, tc)
						tags := p.withServiceClusterNamespace(resource, mapAttrs)
						sketchCache.Update(lex.MetricName, tags, lr, false, resource)
					}

					if len(lex.AggregateDimensions) > 0 {
						mapAttrs := lex.ExtractAggregateAttributes(ctx, tc)
						tags := p.withServiceClusterNamespace(resource, mapAttrs)
						sketchCache.Update(lex.MetricName, tags, lr, true, resource)
					}
				}
			}
		}
	}
}
