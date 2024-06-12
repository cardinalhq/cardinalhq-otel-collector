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
	m := pmetric.NewMetrics()
	points := &pointRecord{Now: time.Now().UnixMilli()}

	for _, metric := range ddMetrics {
		if err := ddr.convertMetricV2(m, metric, points); err != nil {
			return err
		}
		count++
		if count > 100 {
			if err := ddr.nextMetricConsumer.ConsumeMetrics(ctx, m); err != nil {
				return err
			}
			m = pmetric.NewMetrics()
			count = 0
		}
	}

	ddr.metricLogger.Info("received metrics", zap.Any("times", points))
	if count == 0 {
		return nil
	}
	return ddr.nextMetricConsumer.ConsumeMetrics(context.Background(), m)
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

type pointRecord struct {
	Now        int64 `json:"now,omitempty" yaml:"now,omitempty"`
	Total      int64 `json:"total,omitempty" yaml:"total,omitempty"`
	OldestTS   int64 `json:"oldestTS,omitempty" yaml:"oldestTS,omitempty"`
	WorstDelay int64 `json:"worstDelay,omitempty" yaml:"worstDelay,omitempty"`
}

func (pr *pointRecord) record(pointTime int64, n int64) {
	late := pr.Now - pointTime
	if late > pr.WorstDelay {
		pr.WorstDelay = late
	}
	if pr.OldestTS == 0 || pointTime < pr.OldestTS {
		pr.OldestTS = pointTime
	}
	pr.Total += n
}

func (ddr *datadogReceiver) convertMetricV2(m pmetric.Metrics, v2 *ddpb.MetricPayload_MetricSeries, pr *pointRecord) error {
	rm := m.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(semconv.SchemaURL)
	rAttr := rm.Resource().Attributes()
	scope := rm.ScopeMetrics().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	sAttr.PutStr(string(semconv.AttributeTelemetrySDKName), "Datadog")

	metric := scope.Metrics().AppendEmpty()
	metric.SetName(v2.Metric)
	metric.SetUnit(v2.Unit)

	if v2.Metadata != nil && v2.Metadata.Origin != nil {
		rAttr.PutInt("dd.origin.category", int64(v2.Metadata.Origin.OriginCategory))
		rAttr.PutInt("dd.origin.product", int64(v2.Metadata.Origin.OriginProduct))
		rAttr.PutInt("dd.origin.service", int64(v2.Metadata.Origin.OriginService))
	}

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
		g := metric.SetEmptyGauge()
		for _, point := range v2.Points {
			gdp := g.DataPoints().AppendEmpty()
			lAttr.CopyTo(gdp.Attributes())
			populateDatapoint(&gdp, point.Timestamp*1000, &v2.Interval, point.Value)
			pr.record(point.Timestamp*1000, 1)
		}
	case ddpb.MetricPayload_RATE:
		g := metric.SetEmptyGauge()
		for _, point := range v2.Points {
			gdp := g.DataPoints().AppendEmpty()
			lAttr.CopyTo(gdp.Attributes())
			gdp.Attributes().PutBool("dd.israte", true)
			gdp.Attributes().PutInt("dd.rateInterval", v2.Interval)
			populateDatapoint(&gdp, point.Timestamp*1000, &v2.Interval, point.Value)
			pr.record(point.Timestamp*1000, 1)
		}
	case ddpb.MetricPayload_COUNT:
		c := metric.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, point := range v2.Points {
			cdp := c.DataPoints().AppendEmpty()
			lAttr.CopyTo(cdp.Attributes())
			populateDatapoint(&cdp, point.Timestamp*1000, &v2.Interval, point.Value)
			pr.record(point.Timestamp*1000, 1)
		}
	}

	return nil
}
