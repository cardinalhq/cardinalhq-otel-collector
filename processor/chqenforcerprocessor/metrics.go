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

package chqenforcerprocessor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *chqEnforcer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	now := time.Now()
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		serviceName := getServiceName(rm.Resource().Attributes())
		rattr := rm.Resource().Attributes()
		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			sattr := ilm.Scope().Attributes()
			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				metricName := m.Name()
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return e.aggregate(rm, ilm, m, dp)
					})
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return e.aggregate(rm, ilm, m, dp)
					})
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				}

				return false
			})
			return ilm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	e.emit()

	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

func (e *chqEnforcer) processDatapoint(now time.Time, metricName, serviceName string, rattr, sattr, dattr pcommon.Map) {
	if err := e.recordDatapoint(now, metricName, serviceName, rattr, sattr, dattr); err != nil {
		e.logger.Error("Failed to record datapoint", zap.Error(err))
	}
	if e.config.DropDecorationAttributes {
		removeCardinalFields(dattr)
	}
}

func computeStatsOnField(k string) bool {
	if strings.HasPrefix(k, translate.CardinalFieldTID) {
		return true
	}
	return !strings.HasPrefix(k, translate.CardinalFieldPrefixDot)
}

func (e *chqEnforcer) recordDatapoint(now time.Time, metricName, serviceName string, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error

	rattr.Range(func(k string, v pcommon.Value) bool {
		if !computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "resource."+k, v.AsString(), 1))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if !computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "scope."+k, v.AsString(), 1))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if !computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "metric."+k, v.AsString(), 1))
		}
		return true
	})
	return errs
}

func (e *chqEnforcer) recordMetric(now time.Time, metricName, serviceName, tagName, tagValue string, count int) error {
	rec := &MetricStat{
		MetricName:  metricName,
		TagName:     tagName,
		ServiceName: serviceName,
		Phase:       e.pbPhase,
		VendorID:    e.config.Statistics.Vendor,
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

func (e *chqEnforcer) sendMetricStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*MetricStat) {
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

func (e *chqEnforcer) postMetricStats(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	e.logger.Info("Sending metric stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Statistics.Endpoint + "/api/v1/metricstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

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

func (e *chqEnforcer) updateMetricsamplingConfig(sc sampler.SamplerConfig) {
	e.logger.Info("Updating metric sampling config", zap.String("vendor", e.vendor))
	e.aggregatorF.Configure(sc, e.vendor)
	e.aggregatorI.Configure(sc, e.vendor)
}
