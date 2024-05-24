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
	"encoding/json"
	"net/http"

	"github.com/barweiss/go-tuple"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
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

func (ddr *datadogReceiver) processMetricsV1(ddMetrics []SeriesV1) error {
	for _, metric := range ddMetrics {
		otelMetric, err := ddr.convertMetricV1(metric)
		if err != nil {
			return err
		}
		if err := ddr.nextMetricConsumer.ConsumeMetrics(context.Background(), otelMetric); err != nil {
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

func (ddr *datadogReceiver) convertMetricV1(v1 SeriesV1) (pmetric.Metrics, error) {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(semconv.SchemaURL)
	rAttr := rm.Resource().Attributes()
	rAttr.PutStr(semconv.AttributeHostName, *v1.Host)
	scope := rm.ScopeMetrics().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	sAttr.PutStr(string(semconv.AttributeTelemetrySDKName), "Datadog")

	metric := scope.Metrics().AppendEmpty()
	metric.SetName(v1.Metric)
	metric.SetUnit("1")

	for _, tag := range v1.Tags {
		kv := splitTags(tag)
		for k, v := range kv {
			decorate(k, v, rAttr, sAttr)
		}
	}

	mtype := getMetricType(v1.Type)
	switch mtype {
	case "gauge", "rate":
		if mtype == "rate" {
			ddr.params.Logger.Warn("Rate type is not supported by OpenTelemetry, converting to gauge")
		}
		g := metric.SetEmptyGauge()
		for _, point := range v1.Points {
			dp := g.DataPoints().AppendEmpty()
			populateDatapoint(&dp, point.V1, v1.Interval, point.V2)
		}
	case "count":
		c := metric.SetEmptySum()
		c.SetIsMonotonic(false)
		c.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, point := range v1.Points {
			dp := c.DataPoints().AppendEmpty()
			populateDatapoint(&dp, point.V1, v1.Interval, point.V2)
		}
	}

	return m, nil
}

func populateDatapoint(dp *pmetric.NumberDataPoint, ts int64, interval *int64, value float64) {
	dp.SetTimestamp(pcommon.Timestamp(uint64(ts) * 1_000_000))
	dp.SetDoubleValue(value)
	if interval != nil {
		starttime := uint64(ts) - uint64(*interval)
		dp.SetStartTimestamp(pcommon.Timestamp(starttime * 1_000_000))
	}
}
