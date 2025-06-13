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
	"encoding/json"
	"net/http"
	"time"

	"github.com/barweiss/go-tuple"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// MetricsPayloadV1 is for the /api/v1/series endpoint.
type MetricsPayloadV1 struct {
	Series []SeriesV1 `json:"series"`
}

// SeriesV1 is for the /api/v1/series endpoint.
type SeriesV1 struct {
	Host     *string                    `json:"host,omitempty"`
	Interval *int64                     `json:"interval,omitempty"`
	Metric   string                     `json:"metric"`
	Points   []tuple.T2[int64, float64] `json:"points"`
	Tags     []string                   `json:"tags,omitempty"`
	Type     *string                    `json:"type,omitempty"`
}

func handleMetricsV1Payload(req *http.Request) (ret []SeriesV1, httpCode int, err error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return nil, http.StatusUnsupportedMediaType, nil
	}

	wrapper := MetricsPayloadV1{}
	err = json.NewDecoder(req.Body).Decode(&wrapper)
	if err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}
	return wrapper.Series, http.StatusAccepted, nil
}

func (ddr *datadogReceiver) processMetricsV1(ctx context.Context, apikey string, ddMetrics []SeriesV1) error {
	now := time.Now()
	for _, metric := range ddMetrics {
		otelMetric, err := ddr.convertMetricV1(apikey, metric)
		if err != nil {
			return err
		}
		ddr.recordAgeForMetrics(ctx, &otelMetric, now, "v1")

		if err := ddr.nextMetricConsumer.ConsumeMetrics(ctx, otelMetric); err != nil {
			return err
		}
	}
	return nil
}

func getMetricType(mt *string) string {
	if mt == nil {
		return "gauge"
	}
	switch *mt {
	case "gauge", "rate", "count":
		return *mt
	default:
		return "gauge"
	}
}

func (ddr *datadogReceiver) convertMetricV1(apikey string, v1 SeriesV1) (pmetric.Metrics, error) {
	tagCache := newLocalTagCache()

	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(semconv.SchemaURL)
	rAttr := rm.Resource().Attributes()
	scope := rm.ScopeMetrics().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	sAttr.PutStr(string(semconv.TelemetrySDKNameKey), "Datadog")

	hostname := "unknown"
	if v1.Host != nil {
		hostname = *v1.Host
	}

	mm := scope.Metrics().AppendEmpty()
	mm.SetName(v1.Metric)
	mm.SetUnit("1")

	lAttr := pcommon.NewMap()
	kv := splitTagSlice(v1.Tags)
	for k, v := range kv {
		if k == "host" && hostname == "unknown" {
			hostname = v
		}
		decorateItem(k, v, rAttr, sAttr, lAttr)
	}
	rAttr.PutStr(string(semconv.HostNameKey), hostname)

	ddr.hostnameTags.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("hostname", hostname),
		attribute.String("telemetry_type", "metrics"),
		attribute.String("datadog_api_version", "v1"),
	))

	for _, v := range tagCache.FetchCache(ddr.tagcacheExtension, apikey, hostname) {
		rAttr.PutStr(v.Name, v.Value)
	}

	mtype := getMetricType(v1.Type)
	switch mtype {
	case "gauge":
		g := mm.SetEmptyGauge()
		for _, point := range v1.Points {
			dp := g.DataPoints().AppendEmpty()
			lAttr.CopyTo(dp.Attributes())
			populateDatapoint(&dp, point.V1*1000, point.V2)
		}
	case "count":
		c := mm.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, point := range v1.Points {
			dp := c.DataPoints().AppendEmpty()
			lAttr.CopyTo(dp.Attributes())
			populateDatapoint(&dp, point.V1*1000, point.V2)
		}
	case "rate":
		c := mm.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		interval := int64(1)
		if v1.Interval != nil {
			interval = *v1.Interval
		}
		for _, point := range v1.Points {
			dp := c.DataPoints().AppendEmpty()
			lAttr.CopyTo(dp.Attributes())
			populateDatapoint(&dp, point.V1*1000, point.V2*float64(interval))
			dp.Attributes().PutInt("_dd.rateInterval", interval)
		}
	}

	return m, nil
}

func populateDatapoint(dp *pmetric.NumberDataPoint, ts int64, value float64) {
	timestamp := pcommon.Timestamp(uint64(ts) * 1_000_000)
	dp.SetTimestamp(timestamp)
	dp.SetDoubleValue(value)
	dp.SetStartTimestamp(timestamp)
}
