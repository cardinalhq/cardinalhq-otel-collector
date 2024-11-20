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
	"github.com/cardinalhq/cardinalhq-otel-collector/pkg/translate"
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
						e.processDatapoint(md, now, metricName, pmetric.MetricTypeGauge.String(), serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						e.processDatapoint(md, now, metricName, pmetric.MetricTypeSum.String(), serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						e.processDatapoint(md, now, metricName, pmetric.MetricTypeHistogram.String(), serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						e.processDatapoint(md, now, metricName, pmetric.MetricTypeSummary.String(), serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						e.processDatapoint(md, now, metricName, pmetric.MetricTypeExponentialHistogram.String(), serviceName, extra, environment, rattr, sattr, dp.Attributes())
					}
				}
			}
		}
	}

	return md, nil
}

func (e *statsProc) processDatapoint(lm pmetric.Metrics, now time.Time, metricName, metricType, serviceName string, extra map[string]string, environment translate.Environment, rattr, sattr, dattr pcommon.Map) {
	tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
	if err := e.recordDatapoint(lm, now, metricName, metricType, serviceName, tid, rattr, sattr, dattr); err != nil {
		e.logger.Error("Failed to record datapoint", zap.Error(err))
	}
}

func computeStatsOnField(k string) bool {
	if strings.HasPrefix(k, translate.CardinalFieldTID) {
		return true
	}
	return !strings.HasPrefix(k, translate.CardinalFieldPrefixDot)
}

func (e *statsProc) recordDatapoint(lm pmetric.Metrics, now time.Time, metricName, metricType, serviceName string, tid int64, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error

	attributes := e.processEnrichments(map[string]pcommon.Map{
		"resource": rattr,
		"scope":    sattr,
		"metric":   dpAttr,
	})
	e.addMetricsExemplar(lm, serviceName, metricName, metricType)

	rattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, metricType, serviceName, k, v.AsString(), "resource", attributes, 1))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, metricType, serviceName, k, v.AsString(), "scope", attributes, 1))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, metricType, serviceName, k, v.AsString(), "datapoint", attributes, 1))
		}
		return true
	})
	errs = multierr.Append(errs, e.recordMetric(now, metricName, metricType, serviceName, translate.CardinalFieldTID, strconv.FormatInt(tid, 10), "metric", attributes, 1))
	return errs
}

func (e *statsProc) recordMetric(now time.Time, metricName string, metricType string, serviceName string, tagName, tagValue string, tagScope string, attributes []*chqpb.Attribute, count int) error {
	rec := &MetricStat{
		MetricName:  metricName,
		TagName:     tagName,
		TagScope:    tagScope,
		MetricType:  metricType,
		ServiceName: serviceName,
		Phase:       e.pbPhase,
		ProcessorId: e.id.Name(),
		Count:       int64(count),
		Attributes:  attributes,
	}

	bucketpile, err := e.metricstats.Record(now, rec, tagValue, count, 0)
	if err != nil {
		return err
	}
	if bucketpile != nil && len(*bucketpile) > 0 {
		e.exemplarsMu.RLock()

		var marshalledExemplars []*chqpb.MetricExemplar
		for fingerprint, exemplar := range e.metricExemplars {
			b, err := e.jsonMarshaller.metricsMarshaler.MarshalMetrics(exemplar)
			if err != nil {
				e.logger.Error("Failed to marshal metric exemplars", zap.Error(err))
				continue
			}
			split := strings.Split(fingerprint, ":")
			marshalledExemplars = append(marshalledExemplars, &chqpb.MetricExemplar{
				ServiceName: split[0],
				MetricName:  split[1],
				MetricType:  split[2],
				Exemplar:    b,
			})
		}
		e.exemplarsMu.RUnlock()

		// clear exemplars map for next batch
		e.exemplarsMu.Lock()
		e.metricExemplars = make(map[string]pmetric.Metrics)
		e.exemplarsMu.Unlock()

		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendMetricStats(context.Background(), now, bucketpile, marshalledExemplars)
	}
	return nil
}

func (e *statsProc) addMetricsExemplar(lm pmetric.Metrics, serviceName, metricName, metricType string) {
	fingerprint := serviceName + ":" + metricName + ":" + metricType
	e.exemplarsMu.RLock()
	_, found := e.metricExemplars[fingerprint]
	e.exemplarsMu.RUnlock()

	if !found {
		copyObj := pmetric.NewMetrics()
		lm.CopyTo(copyObj)
		// iterate over all 3 levels, and just filter any metrics records that don't match the fingerprint
		copyObj.ResourceMetrics().RemoveIf(func(rsp pmetric.ResourceMetrics) bool {
			rsp.ScopeMetrics().RemoveIf(func(ss pmetric.ScopeMetrics) bool {
				foundFp := false // make sure we only add one record matching the fingerprint.
				incomingServiceName := getServiceName(rsp.Resource().Attributes())
				ss.Metrics().RemoveIf(func(lr pmetric.Metric) bool {
					incomingFingerprint := incomingServiceName + ":" + lr.Name() + ":" + lr.Type().String()
					if incomingFingerprint == fingerprint {
						if !foundFp {
							foundFp = true
							return false
						}
						return true
					}
					return true
				})
				return ss.Metrics().Len() == 0
			})
			return rsp.ScopeMetrics().Len() == 0
		})

		if copyObj.ResourceMetrics().Len() > 0 {
			e.exemplarsMu.Lock()
			e.metricExemplars[fingerprint] = copyObj
			e.exemplarsMu.Unlock()
		}
	}
}

func (e *statsProc) sendMetricStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*MetricStat, marshalledExemplars []*chqpb.MetricExemplar) {
	baseWrapper := &chqpb.MetricStatsReport{
		SubmittedAt: now.UnixMilli(),
		Exemplars:   marshalledExemplars,
	}

	var currentBatch []*chqpb.MetricStats

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
				TagScope:            ms.TagScope,
				MetricType:          ms.MetricType,
				Phase:               ms.Phase,
				Count:               ms.Count,
				ProcessorId:         ms.ProcessorId,
				CardinalityEstimate: estimate,
				Hll:                 b,
				Attributes:          ms.Attributes,
			}
			currentBatch = append(currentBatch, item)

			if len(currentBatch) >= maxBatchSize {
				e.sendBatch(ctx, baseWrapper, currentBatch)
				currentBatch = currentBatch[:0] // Reset batch
			}
		}
	}

	// Send any remaining items in the last batch
	if len(currentBatch) > 0 {
		e.sendBatch(ctx, baseWrapper, currentBatch)
	}
}

func (e *statsProc) sendBatch(ctx context.Context, baseWrapper *chqpb.MetricStatsReport, batch []*chqpb.MetricStats) {
	wrapper := &chqpb.MetricStatsReport{
		SubmittedAt: baseWrapper.SubmittedAt,
		Exemplars:   baseWrapper.Exemplars,
		Stats:       batch,
	}

	if err := e.postMetricStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send metric stats batch", zap.Error(err))
	} else {
		e.logger.Debug("Sent metric stats batch", zap.Int("count", len(batch)))
	}
}

func (e *statsProc) postMetricStats(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	e.statsBatchSize.Record(int64(len(b)))
	e.logger.Debug("Sending metric stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
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
