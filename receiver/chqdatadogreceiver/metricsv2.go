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

package datadogreceiver

import (
	"context"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
			hexdump(buf.Bytes())
			return nil, http.StatusUnprocessableEntity, err
		}
	default:
		ddr.metricLogger.Warn("unsupported content type", zap.String("content-type", req.Header.Get("Content-Type")))
		return nil, http.StatusUnsupportedMediaType, err
	}

	return message.Series, http.StatusAccepted, nil
}

func (ddr *datadogReceiver) processMetricsV2(ctx context.Context, ddMetrics []*ddpb.MetricPayload_MetricSeries) error {
	count := 0
	keptDatapoints := 0
	removedDatapoints := 0
	m := pmetric.NewMetrics()
	now := time.Now()
	tooOld := now.Add(-ddr.config.MaxMetricDatapointAge)

	defer func() {
		asetRemoved := attribute.NewSet(
			attribute.String("apiversion", "v2"),
			attribute.String("disposition", "removed"),
			attribute.String("max_age", ddr.config.MaxMetricDatapointAge.String()),
			attribute.String("received_by_pod", ddr.podName),
			attribute.String("receiver", ddr.id))
		asetKept := attribute.NewSet(
			attribute.String("apiversion", "v2"),
			attribute.String("disposition", "kept"),
			attribute.String("max_age", ddr.config.MaxMetricDatapointAge.String()),
			attribute.String("received_by_pod", ddr.podName),
			attribute.String("receiver", ddr.id))
		ddr.metricFilterCounter.Add(ctx, int64(removedDatapoints), metric.WithAttributeSet(asetRemoved))
		ddr.metricFilterCounter.Add(ctx, int64(keptDatapoints), metric.WithAttributeSet(asetKept))
	}()

	for _, ddMetric := range ddMetrics {
		if err := ddr.convertMetricV2(m, ddMetric); err != nil {
			return err
		}
		count++
		if count > 100 {
			ddr.recordAgeForMetrics(ctx, &m, now, "v2")
			kept, removed := ddr.filterOlderThan(&m, tooOld)
			keptDatapoints += kept
			removedDatapoints += removed
			if kept > 0 {
				if err := ddr.nextMetricConsumer.ConsumeMetrics(ctx, m); err != nil {
					return err
				}
			}
			m = pmetric.NewMetrics()
			count = 0
		}
	}

	if count > 0 && m.DataPointCount() > 0 {
		ddr.recordAgeForMetrics(ctx, &m, now, "v2")
		kept, removed := ddr.filterOlderThan(&m, tooOld)
		keptDatapoints += kept
		removedDatapoints += removed
		if kept > 0 {
			if err := ddr.nextMetricConsumer.ConsumeMetrics(ctx, m); err != nil {
				return err
			}
		}
	}

	return nil
}

func (ddr *datadogReceiver) recordAgeForMetrics(ctx context.Context, m *pmetric.Metrics, now time.Time, apiversion string) {
	for i := 0; i < m.ResourceMetrics().Len(); i++ {
		rm := m.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			imm := rm.ScopeMetrics().At(j)
			for k := 0; k < imm.Metrics().Len(); k++ {
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
				}
				age := now.Sub(ts)
				attrs := metric.WithAttributes(
					attribute.String("telemetry_type", "metric"),
					attribute.String("datadog_api_version", apiversion),
				)
				ddr.datapointAge.Record(ctx, age.Seconds(), metric.WithAttributeSet(ddr.aset), attrs)
			}
		}
	}
}

func (ddr *datadogReceiver) logDrop(rm pmetric.ResourceMetrics, name string, dp pmetric.NumberDataPoint) {
	serviceName := "unknown"
	if v, found := rm.Resource().Attributes().Get("service.name"); found {
		serviceName = v.AsString()
	}
	ddr.metricLogger.Info("dropping data point",
		zap.String("name", name),
		zap.String("service", serviceName),
		zap.Int64("timestamp", dp.Timestamp().AsTime().UnixMilli()))
}

// filterOlderThan removes data points older than the given timestamp.  As this receiver only processes
// gauges and counters, it only removes data points from those types.  All other types will be kept.
func (ddr *datadogReceiver) filterOlderThan(m *pmetric.Metrics, ts time.Time) (keptDatapoints, removedDatapoints int) {
	firstDrop := true
	targetTime := pcommon.NewTimestampFromTime(ts)
	m.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			ilm.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					metric.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						if dp.Timestamp() < targetTime {
							if firstDrop {
								ddr.logDrop(rm, metric.Name(), dp)
								firstDrop = false
							}
							removedDatapoints++
							return true
						}
						keptDatapoints++
						return false
					})
					return metric.Gauge().DataPoints().Len() == 0
				case pmetric.MetricTypeSum:
					metric.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						if dp.Timestamp() < targetTime {
							if firstDrop {
								ddr.logDrop(rm, metric.Name(), dp)
								firstDrop = false
							}
							removedDatapoints++
							return true
						}
						keptDatapoints++
						return false
					})
					return metric.Sum().DataPoints().Len() == 0
				default:
					return false
				}
			})
			return ilm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	return
}

func ensureServiceName(rAttr pcommon.Map, kv map[string]string) {
	if _, ok := rAttr.Get("service.name"); ok {
		return
	}
	searchPath := []string{"kube_deployment", "kube_stateful_set", "kube_daemon_set", "container_name", "short_image"}
	for _, path := range searchPath {
		if v, ok := kv[path]; ok {
			rAttr.PutStr("service.name", v)
			return
		}
	}
	rAttr.PutStr("service.name", "unknown")
}

func (ddr *datadogReceiver) convertMetricV2(m pmetric.Metrics, v2 *ddpb.MetricPayload_MetricSeries) error {
	rm := m.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(semconv.SchemaURL)
	rAttr := rm.Resource().Attributes()
	scope := rm.ScopeMetrics().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	sAttr.PutStr(string(semconv.AttributeTelemetrySDKName), "Datadog")

	ddMetric := scope.Metrics().AppendEmpty()
	ddMetric.SetName(v2.Metric)
	ddMetric.SetUnit(v2.Unit)

	// if v2.Metadata != nil && v2.Metadata.Origin != nil {
	// 	rAttr.PutInt("dd.origin.category", int64(v2.Metadata.Origin.OriginCategory))
	// 	rAttr.PutInt("dd.origin.product", int64(v2.Metadata.Origin.OriginProduct))
	// 	rAttr.PutInt("dd.origin.service", int64(v2.Metadata.Origin.OriginService))
	// }

	kvTags := map[string]string{}
	lAttr := pcommon.NewMap()
	for _, tag := range v2.Tags {
		item := splitTags(tag)
		for k, v := range item {
			kvTags[k] = v
		}
	}
	for k, v := range kvTags {
		decorateItem(k, v, rAttr, sAttr, lAttr)
	}
	if v2.Resources != nil {
		for _, resource := range v2.Resources {
			decorate(resource.Type, resource.Name, rAttr, sAttr)
		}
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
