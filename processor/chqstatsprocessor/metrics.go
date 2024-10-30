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

package chqstatsprocessor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *statsProc) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	now := time.Now()
	environment := translate.EnvironmentFromEnv()

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		serviceName := getServiceName(rm.Resource().Attributes())
		rattr := rm.Resource().Attributes()
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			sattr := ilm.Scope().Attributes()
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				metricName := m.Name()
				extra := map[string]string{"name": m.Name()}

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						e.processDatapoint(now, metricName, serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						e.processDatapoint(now, metricName, serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						e.processDatapoint(now, metricName, serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						e.processDatapoint(now, metricName, serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						e.processDatapoint(now, metricName, serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				}
			}
		}
	}

	return md, nil
}

func (e *statsProc) processDatapoint(now time.Time, metricName, serviceName string, extra map[string]string, environment translate.Environment, rattr, sattr, dattr pcommon.Map) {
	tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
	if err := e.recordDatapoint(now, metricName, serviceName, tid, rattr, sattr, dattr); err != nil {
		e.logger.Error("Failed to record datapoint", zap.Error(err))
	}
}

func computeStatsOnField(k string) bool {
	if strings.HasPrefix(k, translate.CardinalFieldTID) {
		return true
	}
	return !strings.HasPrefix(k, translate.CardinalFieldPrefixDot)
}

func (e *statsProc) recordDatapoint(now time.Time, metricName, serviceName string, tid int64, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error

	attributes := e.processEnrichments(map[string]pcommon.Map{
		"resource": rattr,
		"scope":    sattr,
		"metric":   dpAttr,
	})
	rattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "resource."+k, v.AsString(), attributes, 1))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "scope."+k, v.AsString(), attributes, 1))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "metric."+k, v.AsString(), attributes, 1))
		}
		return true
	})
	errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "metric."+translate.CardinalFieldTID, strconv.FormatInt(tid, 10), attributes, 1))
	return errs
}

func (e *statsProc) recordMetric(now time.Time, metricName string, serviceName string, tagName, tagValue string, attributes []*chqpb.Attribute, count int) error {
	rec := &MetricStat{
		MetricName:  metricName,
		TagName:     tagName,
		ServiceName: serviceName,
		Phase:       e.pbPhase,
		VendorID:    e.config.Statistics.Vendor,
		Count:       int64(count),
		Attributes:  attributes,
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

func (e *statsProc) sendMetricStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*MetricStat) {
	wrapper := &chqpb.MetricStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.MetricStats{},
	}

	for _, stats := range *bucketpile {
		for _, ms := range stats {
			if ms.HLL == nil {
				e.logger.Error("HLL is nil", zap.Any("metric", ms))
				continue
			}
			estimate, _ := ms.HLL.GetEstimate() // ignore error for now
			b, err := ms.HLL.ToCompactSlice()
			if err != nil {
				e.logger.Error("Failed to convert HLL to compact slice", zap.Error(err))
				continue
			}
			item := &chqpb.MetricStats{
				MetricName:          ms.MetricName,
				ServiceName:         ms.ServiceName,
				TagName:             ms.TagName,
				Phase:               ms.Phase,
				Count:               ms.Count,
				VendorId:            ms.VendorID,
				CardinalityEstimate: estimate,
				Hll:                 b,
				Attributes:          ms.Attributes,
			}
			wrapper.Stats = append(wrapper.Stats, item)
		}
	}

	if err := e.postMetricStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send metric stats", zap.Error(err))
	}
	e.logger.Info("Sent metric stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *statsProc) postMetricStats(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
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
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send metric stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
