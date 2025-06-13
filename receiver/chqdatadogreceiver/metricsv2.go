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

package datadogreceiver

import (
	"context"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ddpb "github.com/cardinalhq/cardinalhq-otel-collector/internal/ddpb"
)

const maxreceivesize = 5 * 1024 * 1024 // 5MB

func (ddr *datadogReceiver) handleMetricsV2Payload(req *http.Request) (ret []*ddpb.MetricPayload_MetricSeries, httpCode int, err error) {
	buf := getBuffer()
	defer putBuffer(buf)

	n, err := io.Copy(buf, req.Body)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	if n > maxreceivesize {
		return nil, http.StatusRequestEntityTooLarge, err
	}

	var message ddpb.MetricPayload
	switch req.Header.Get("Content-Type") {
	case "application/json":
		if err := protojson.Unmarshal(buf.Bytes(), &message); err != nil {
			return nil, http.StatusUnprocessableEntity, err
		}
	case "application/x-protobuf":
		if err := proto.Unmarshal(buf.Bytes(), &message); err != nil {
			return nil, http.StatusUnprocessableEntity, err
		}
	default:
		ddr.metricLogger.Warn("unsupported content type", zap.String("content-type", req.Header.Get("Content-Type")))
		return nil, http.StatusUnsupportedMediaType, err
	}

	return message.Series, http.StatusAccepted, nil
}

func (ddr *datadogReceiver) processMetricsV2(ctx context.Context, apikey string, ddMetrics []*ddpb.MetricPayload_MetricSeries) error {
	count := 0
	m := pmetric.NewMetrics()
	now := time.Now()

	for _, ddMetric := range ddMetrics {
		if err := ddr.convertMetricV2(apikey, m, ddMetric); err != nil {
			return err
		}
		count++
		if count > 100 {
			ddr.recordAgeForMetrics(ctx, &m, now, "v2")
			if err := ddr.nextMetricConsumer.ConsumeMetrics(ctx, m); err != nil {
				return err
			}
			m = pmetric.NewMetrics()
			count = 0
		}
	}

	if count > 0 && m.DataPointCount() > 0 {
		ddr.recordAgeForMetrics(ctx, &m, now, "v2")
		if err := ddr.nextMetricConsumer.ConsumeMetrics(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

func (ddr *datadogReceiver) recordAgeForMetrics(ctx context.Context, m *pmetric.Metrics, now time.Time, apiversion string) {
	for i := range m.ResourceMetrics().Len() {
		rm := m.ResourceMetrics().At(i)
		for j := range rm.ScopeMetrics().Len() {
			imm := rm.ScopeMetrics().At(j)
			for k := range imm.Metrics().Len() {
				m := imm.Metrics().At(k)
				var ts time.Time
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					ts = m.Gauge().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeSum:
					ts = m.Sum().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeHistogram:
					ts = m.Histogram().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeSummary:
					ts = m.Summary().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeExponentialHistogram:
					ts = m.ExponentialHistogram().DataPoints().At(0).Timestamp().AsTime()
				case pmetric.MetricTypeEmpty:
					continue
				default:
					ddr.metricLogger.Warn("unknown metric type", zap.String("type", m.Type().String()))
					continue
				}
				age := now.Sub(ts)
				incomingClientID, incomingCollectorID := getClientIDs(ctx)
				attrs := metric.WithAttributes(
					attribute.String("telemetry_type", "metrics"),
					attribute.String("datadog_api_version", apiversion),
					attribute.String("chq_incoming_client_id", incomingClientID),
					attribute.String("chq_incoming_collector_id", incomingCollectorID),
				)
				ddr.datapointAge.Record(ctx, age.Seconds(), metric.WithAttributeSet(ddr.aset), attrs)
			}
		}
	}
}

func getClientIDs(ctx context.Context) (string, string) {
	clientID := getAuthString(ctx, "client_id")
	if clientID == "" {
		clientID = "not-set"
	}
	collectorID := getAuthString(ctx, "collector_id")
	if collectorID == "" {
		collectorID = "not-set"
	}
	return clientID, collectorID
}

func getAuthString(ctx context.Context, key string) string {
	cl := client.FromContext(ctx)
	if cl.Auth == nil {
		return ""
	}
	v := cl.Auth.GetAttribute(key)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func ensureServiceName(rAttr pcommon.Map, kv map[string]string) {
	if _, ok := rAttr.Get("service.name"); ok {
		return
	}
	searchPath := []string{"service", "kube_deployment", "kube_stateful_set", "kube_daemon_set", "short_image", "container_name"}
	for _, path := range searchPath {
		if v, ok := kv[path]; ok {
			rAttr.PutStr("service.name", v)
			return
		}
	}
	rAttr.PutStr("service.name", "unknown")
}

func (ddr *datadogReceiver) convertMetricV2(apikey string, m pmetric.Metrics, v2 *ddpb.MetricPayload_MetricSeries) error {
	rm := m.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(semconv.SchemaURL)
	rAttr := rm.Resource().Attributes()
	scope := rm.ScopeMetrics().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	sAttr.PutStr(string(semconv.TelemetrySDKNameKey), "Datadog")

	ddMetric := scope.Metrics().AppendEmpty()
	ddMetric.SetName(v2.Metric)
	ddMetric.SetUnit(v2.Unit)

	// if v2.Metadata != nil && v2.Metadata.Origin != nil {
	// 	rAttr.PutInt("dd.origin.category", int64(v2.Metadata.Origin.OriginCategory))
	// 	rAttr.PutInt("dd.origin.product", int64(v2.Metadata.Origin.OriginProduct))
	// 	rAttr.PutInt("dd.origin.service", int64(v2.Metadata.Origin.OriginService))
	// }

	kvTags := splitTagSlice(v2.Tags)

	hostname := "unknown"
	targets := []string{"host.name", "hostname", "host"}
	for _, target := range targets {
		if v, ok := kvTags[target]; ok && v != "" {
			hostname = v // no break, allow better values to match
		}
	}

	lAttr := pcommon.NewMap()
	for k, v := range kvTags {
		decorateItem(k, v, rAttr, sAttr, lAttr)
	}
	if v2.Resources != nil {
		for _, resource := range v2.Resources {
			decorate(resource.Type, resource.Name, rAttr, sAttr)
			if resource.Type == "host" {
				hostname = resource.Name
			}
		}
	}
	ddr.hostnameTags.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("hostname", hostname),
		attribute.String("telemetry_type", "metrics"),
		attribute.String("datadog_api_version", "v2"),
	))
	tagCache := newLocalTagCache()
	for _, v := range tagCache.FetchCache(ddr.tagcacheExtension, apikey, hostname) {
		rAttr.PutStr(v.Name, v.Value)
	}
	ensureServiceName(rAttr, kvTags)

	switch v2.Type {
	case ddpb.MetricPayload_GAUGE, ddpb.MetricPayload_UNSPECIFIED:
		g := ddMetric.SetEmptyGauge()
		for _, point := range v2.Points {
			gdp := g.DataPoints().AppendEmpty()
			lAttr.CopyTo(gdp.Attributes())
			populateDatapoint(&gdp, point.Timestamp*1000, point.Value)
		}
	case ddpb.MetricPayload_RATE:
		c := ddMetric.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, point := range v2.Points {
			cdp := c.DataPoints().AppendEmpty()
			lAttr.CopyTo(cdp.Attributes())
			cdp.Attributes().PutInt("_dd.rateInterval", v2.Interval)
			populateDatapoint(&cdp, point.Timestamp*1000, point.Value*float64(v2.Interval))
		}
	case ddpb.MetricPayload_COUNT:
		c := ddMetric.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, point := range v2.Points {
			cdp := c.DataPoints().AppendEmpty()
			lAttr.CopyTo(cdp.Attributes())
			populateDatapoint(&cdp, point.Timestamp*1000, point.Value)
		}
	}

	return nil
}
