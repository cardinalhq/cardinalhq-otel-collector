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
	"compress/gzip"
	"context"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ddpb "github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver/internal/ddpb"
)

var bpool = NewBufferPool("datadog-receiver", 5*1024*1024) // 5MB buffer pool (max message size)

func (ddr *datadogReceiver) handleMetricsV2Payload(req *http.Request) (ret []*ddpb.MetricPayload_MetricSeries, httpCode int, err error) {
	buf := bpool.Get()
	defer bpool.Put(buf)

	var from io.ReadCloser = req.Body
	defer req.Body.Close()
	if req.Header.Get("Content-Encoding") == "gzip" {
		from, err = gzip.NewReader(req.Body)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		defer from.Close()
	}

	rl := io.LimitReader(from, int64(bpool.size))
	n, err := rl.Read(buf.buf)
	if n == 0 || err != nil && err != io.EOF {
		return nil, http.StatusUnprocessableEntity, err
	}
	buf.buf = buf.buf[:n]

	if n >= bpool.size {
		return nil, http.StatusRequestEntityTooLarge, err
	}
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	var message ddpb.MetricPayload

	switch req.Header.Get("Content-Type") {
	case "application/json":
		if err := protojson.Unmarshal(buf.buf, &message); err != nil {
			return nil, http.StatusUnprocessableEntity, err
		}
	case "application/x-protobuf":
		if err := proto.Unmarshal(buf.buf, &message); err != nil {
			return nil, http.StatusUnprocessableEntity, err
		}
	default:
		ddr.params.Logger.Warn("unsupported content type", zap.String("content-type", req.Header.Get("Content-Type")))
		return nil, http.StatusUnsupportedMediaType, err
	}

	return message.Series, 0, nil
}

func (ddr *datadogReceiver) processMetricsV2(ddMetrics []*ddpb.MetricPayload_MetricSeries) error {
	for _, metric := range ddMetrics {
		otelMetric, err := ddr.convertMetricV2(metric)
		if err != nil {
			return err
		}
		if err := ddr.nextMetricConsumer.ConsumeMetrics(context.Background(), otelMetric); err != nil {
			return err
		}
	}
	return nil
}

func (ddr *datadogReceiver) convertMetricV2(v2 *ddpb.MetricPayload_MetricSeries) (pmetric.Metrics, error) {
	m := pmetric.NewMetrics()
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

	for _, tag := range v2.Tags {
		kv := splitTags(tag)
		for k, v := range kv {
			decorate(k, v, rAttr, sAttr)
		}
	}

	if v2.Resources != nil {
		for _, resource := range v2.Resources {
			decorate(resource.Name, resource.Type, rAttr, sAttr)
		}
	}

	switch v2.Type {
	case ddpb.MetricPayload_GAUGE, ddpb.MetricPayload_RATE, ddpb.MetricPayload_UNSPECIFIED:
		if v2.Type == ddpb.MetricPayload_RATE {
			ddr.params.Logger.Warn("Rate metric type is not supported in OTLP, converting to gauge")
		}
		g := metric.SetEmptyGauge()
		for _, point := range v2.Points {
			gdp := g.DataPoints().AppendEmpty()
			populateDatapoint(&gdp, point.Timestamp, &v2.Interval, point.Value)
		}
	case ddpb.MetricPayload_COUNT:
		c := metric.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, point := range v2.Points {
			cdp := c.DataPoints().AppendEmpty()
			populateDatapoint(&cdp, point.Timestamp, &v2.Interval, point.Value)
		}
	}

	return m, nil
}
