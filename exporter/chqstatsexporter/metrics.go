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

package chqstatsexporter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *statsExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	now := time.Now()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		serviceName := getServiceName(rm.Resource().Attributes())
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						if err := e.recordDatapoint(now, m.Name(), serviceName, rm.Resource().Attributes(), ilm.Scope().Attributes(), dp.Attributes()); err != nil {
							return err
						}
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						if err := e.recordDatapoint(now, m.Name(), serviceName, rm.Resource().Attributes(), ilm.Scope().Attributes(), dp.Attributes()); err != nil {
							return err
						}
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						if err := e.recordDatapoint(now, m.Name(), serviceName, rm.Resource().Attributes(), ilm.Scope().Attributes(), dp.Attributes()); err != nil {
							return err
						}
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						if err := e.recordDatapoint(now, m.Name(), serviceName, rm.Resource().Attributes(), ilm.Scope().Attributes(), dp.Attributes()); err != nil {
							return err
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						if err := e.recordDatapoint(now, m.Name(), serviceName, rm.Resource().Attributes(), ilm.Scope().Attributes(), dp.Attributes()); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func getBoolOrDefault(attr pcommon.Map, key string, def bool) bool {
	if v, found := attr.Get(key); found {
		if b, ok := v.AsRaw().(bool); ok {
			return b
		}
	}
	return def
}

func (e *statsExporter) recordDatapoint(now time.Time, metricName, serviceName string, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error
	isAggregationOutput := e.pbPhase == chqpb.Phase_PRE && getBoolOrDefault(dpAttr, translate.CardinalFieldAggregatedOutput, false)
	if isAggregationOutput {
		return nil
	}

	rattr.Range(func(k string, v pcommon.Value) bool {
		errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "resource."+k, v.AsString(), 1))
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "scope."+k, v.AsString(), 1))
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "metric."+k, v.AsString(), 1))
		return true
	})
	return errs
}

func (e *statsExporter) recordMetric(now time.Time, metricName, serviceName, tagName, tagValue string, count int) error {
	rec := &MetricStat{
		MetricName:  metricName,
		ServiceName: serviceName,
		TagName:     tagName,
		Phase:       e.pbPhase,
		Count:       int64(count),
	}

	bucketpile, err := e.metricstats.Record(now, rec, tagValue, count, 0)
	if err != nil {
		return err
	}
	if bucketpile != nil {
		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendMetricStats(context.Background(), now, bucketpile)
	}
	return nil
}

func (e *statsExporter) sendMetricStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*MetricStat) {
	wrapper := &chqpb.MetricStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.MetricStats{},
	}

	for _, stats := range *bucketpile {
		for _, ms := range stats {
			item := &chqpb.MetricStats{
				MetricName:  ms.MetricName,
				ServiceName: ms.ServiceName,
				TagName:     ms.TagName,
				Phase:       ms.Phase,
				Count:       ms.Count,
				VendorId:    ms.VendorID,
			}
			if ms.HLL == nil {
				e.logger.Error("HLL is nil", zap.Any("metric", ms))
				continue
			}
			b, err := ms.HLL.ToCompactSlice()
			if err != nil {
				e.logger.Error("Failed to convert HLL to compact slice", zap.Error(err))
				continue
			}
			item.Hll = b
			wrapper.Stats = append(wrapper.Stats, item)
		}
	}

	if err := e.postMetricStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send metric stats", zap.Error(err))
	}
}

func (e *statsExporter) postMetricStats(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	e.logger.Info("Sending metric stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Endpoint + "/api/v1/metricstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set(translate.CardinalHeaderAPIKey, string(e.config.APIKey))

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
