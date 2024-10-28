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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					mlist := e.convertGaugeMetric(ctx, metric, rAttr, sAttr, metric.Gauge())
					msg.Series = append(msg.Series, mlist...)
				case pmetric.MetricTypeSum:
					mlist := e.convertSumMetric(ctx, metric, rAttr, sAttr, metric.Sum())
					msg.Series = append(msg.Series, mlist...)
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
		if err := e.sendMetrics(context.Background(), msg); err != nil {
			return err
		}
	}
	return nil
}

func valueAsFloat64(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	}
	return 0
}

func (e *datadogExporter) convertGaugeMetric(_ context.Context, metric pmetric.Metric, rAttr, sAttr pcommon.Map, g pmetric.Gauge) []*ddpb.MetricPayload_MetricSeries {
	ret := []*ddpb.MetricPayload_MetricSeries{}
	for i := 0; i < g.DataPoints().Len(); i++ {
		m := &ddpb.MetricPayload_MetricSeries{
			Metric: metric.Name(),
			Unit:   metric.Unit(),
			Type:   ddpb.MetricPayload_GAUGE,
		}
		dp := g.DataPoints().At(i)
		tags, resources := tagStrings(rAttr, sAttr, dp.Attributes())
		m.Tags = append(m.Tags, tags...)
		if len(resources) > 0 {
			m.Tags = append(m.Tags, resources...) // TODO this should not be needed but datadog does not seem to add resources to the metric
			for _, resource := range resources {
				i := strings.Split(resource, ":")
				m.Resources = append(m.Resources, &ddpb.MetricPayload_Resource{
					Type: i[0],
					Name: i[1],
				})
			}
		}
		m.Points = append(m.Points, &ddpb.MetricPayload_MetricPoint{
			Timestamp: dp.Timestamp().AsTime().Unix(),
			Value:     valueAsFloat64(dp),
		})
		ret = append(ret, m)
	}
	return ret
}

func (e *datadogExporter) convertSumMetric(_ context.Context, metric pmetric.Metric, rAttr, sAttr pcommon.Map, s pmetric.Sum) []*ddpb.MetricPayload_MetricSeries {
	ret := []*ddpb.MetricPayload_MetricSeries{}
	for i := 0; i < s.DataPoints().Len(); i++ {
		m := &ddpb.MetricPayload_MetricSeries{
			Metric: metric.Name(),
			Unit:   metric.Unit(),
			Type:   ddpb.MetricPayload_COUNT,
		}
		dp := s.DataPoints().At(i)
		value := valueAsFloat64(dp)
		lAttr := dp.Attributes()
		interval, hasInterval := getInterval(lAttr)
		if hasInterval {
			value = value / float64(interval)
			m.Type = ddpb.MetricPayload_RATE
			m.Interval = interval
		}
		lAttr.Remove("_dd.rateInterval")
		tags, resources := tagStrings(rAttr, sAttr, dp.Attributes())
		m.Tags = append(m.Tags, tags...)
		if len(resources) > 0 {
			m.Tags = append(m.Tags, resources...) // TODO this should not be needed but datadog does not seem to add resources to the metric
			for _, resource := range resources {
				i := strings.Split(resource, ":")
				m.Resources = append(m.Resources, &ddpb.MetricPayload_Resource{
					Type: i[0],
					Name: i[1],
				})
			}
		}
		m.Points = append(m.Points, &ddpb.MetricPayload_MetricPoint{
			Timestamp: dp.Timestamp().AsTime().Unix(),
			Value:     value,
		})
		ret = append(ret, m)
	}
	return ret
}

func getInterval(lAttr pcommon.Map) (int64, bool) {
	if v, found := lAttr.Get("_dd.rateInterval"); found {
		val := v.AsRaw()
		if intval, ok := val.(int64); ok {
			if intval <= 0 {
				intval = 1
			}
			return intval, true
		}
	}
	return 1, false
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
		if !errors.Is(err, io.EOF) {
			return err
		}
		urlerr, ok := err.(*url.Error)
		if !ok {
			return err
		}
		e.logger.Info("failed to send metrics due to EOF while reading response, ignoring",
			zap.String("op", urlerr.Op),
			zap.String("url", urlerr.URL),
			zap.Bool("timeout", urlerr.Timeout()),
			zap.Bool("temporary", urlerr.Temporary()))
		return nil
	}
	defer func() {
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}()
	e.messagesSubmitted.Add(ctx, int64(len(msg.Series)), metric.WithAttributeSet(e.commonAttributes), metric.WithAttributes(attribute.Int("http.code", resp.StatusCode)))
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send metrics, status code: %d", resp.StatusCode)
	}
	return nil
}
