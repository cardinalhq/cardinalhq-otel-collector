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

package chqdatadogexporter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	ddpb "github.com/cardinalhq/cardinalhq-otel-collector/internal/ddpb"
)

func (e *datadogExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	msg := &ddpb.MetricPayload{}
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		rAttr := pcommon.NewMap()
		rm.Resource().Attributes().CopyTo(rAttr)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			sAttr := pcommon.NewMap()
			ilm.Scope().Attributes().CopyTo(sAttr)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				metric := ilm.Metrics().At(k)
				m := &ddpb.MetricPayload_MetricSeries{
					Metric: metric.Name(),
					Unit:   metric.Unit(),
				}
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					if err := e.convertGaugeMetric(ctx, m, rAttr, sAttr, metric.Gauge()); err != nil {
						return err
					}
					m.Type = ddpb.MetricPayload_GAUGE
					msg.Series = append(msg.Series, m)
				case pmetric.MetricTypeSum:
					if err := e.convertSumMetric(ctx, m, rAttr, sAttr, metric.Sum()); err != nil {
						return err
					}
					m.Type = ddpb.MetricPayload_COUNT
					msg.Series = append(msg.Series, m)
				case pmetric.MetricTypeHistogram:
					//
				case pmetric.MetricTypeExponentialHistogram:
					//
				case pmetric.MetricTypeSummary:
					//
				}
			}
		}
	}

	e.messagesReceived.Add(ctx, int64(len(msg.Series)), metric.WithAttributeSet(e.commonAttributes))
	if len(msg.Series) > 0 {
		e.logger.Info("Sending metrics to Datadog", zap.Int("seriesCount", len(msg.Series)))
		if err := e.sendMetrics(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (e *datadogExporter) convertGaugeMetric(_ context.Context, m *ddpb.MetricPayload_MetricSeries, rAttr, sAttr pcommon.Map, g pmetric.Gauge) error {
	for i := 0; i < g.DataPoints().Len(); i++ {
		dp := g.DataPoints().At(i)
		lAttr := pcommon.NewMap()
		dp.Attributes().CopyTo(lAttr)
		if i == 0 {
			if _, found := lAttr.Get("dd.israte"); found {
				m.Type = ddpb.MetricPayload_RATE
			}
			m.Tags = append(m.Tags, tagStrings(rAttr, sAttr, lAttr)...)
		}
		lAttr.Remove("dd.israte")
		m.Points = append(m.Points, &ddpb.MetricPayload_MetricPoint{
			Timestamp: dp.Timestamp().AsTime().UnixNano(),
			Value:     dp.DoubleValue(),
		})
	}
	return nil
}

func (e *datadogExporter) convertSumMetric(_ context.Context, m *ddpb.MetricPayload_MetricSeries, rAttr, sAttr pcommon.Map, s pmetric.Sum) error {
	for i := 0; i < s.DataPoints().Len(); i++ {
		dp := s.DataPoints().At(i)
		if i == 0 {
			m.Tags = append(m.Tags, tagStrings(rAttr, sAttr, dp.Attributes())...)
		}
		m.Points = append(m.Points, &ddpb.MetricPayload_MetricPoint{
			Timestamp: dp.Timestamp().AsTime().UnixNano(),
			Value:     dp.DoubleValue(),
		})
	}
	return nil
}

func (e *datadogExporter) sendMetrics(ctx context.Context, msg *ddpb.MetricPayload) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	target := e.endpoint + "/api/v2/series"
	req, err := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("DD-API-KEY", e.apiKey)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	e.messagesSubmitted.Add(ctx, int64(len(msg.Series)), metric.WithAttributeSet(e.commonAttributes), metric.WithAttributes(attribute.Int("http.code", resp.StatusCode)))
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send metrics, status code: %d", resp.StatusCode)
	}
	return nil
}
