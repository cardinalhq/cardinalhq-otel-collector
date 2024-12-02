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

	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
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
	rec := &chqpb.MetricStats{
		MetricName:  metricName,
		TagName:     tagName,
		TagScope:    tagScope,
		MetricType:  metricType,
		ServiceName: serviceName,
		Phase:       e.pbPhase,
		ProcessorId: e.id.Name(),
		Count:       int64(count),
		Attributes:  attributes,
		TsHour:      now.Truncate(time.Hour).UnixMilli(),
	}

	stats, err := e.metricstats.Record(rec, tagValue, now)
	telemetry.HistogramRecord(e.recordLatency, int64(time.Since(now)))
	if err != nil {
		return err
	}
	if stats != nil && len(stats) > 0 {
		e.exemplarsMu.Lock()
		defer e.exemplarsMu.Unlock()

		var marshalledExemplars []*chqpb.MetricExemplar
		for fingerprint, exemplar := range e.metricExemplars {
			split := strings.Split(fingerprint, ":")
			sName := split[0]
			mName := split[1]
			mType := split[2]

			copyObj := pmetric.NewMetrics()
			exemplar.CopyTo(copyObj)
			// iterate over all 3 levels, and just filter any metrics records that don't match the fingerprint
			foundFp := false // make sure we only add one record matching the fingerprint.
			copyObj.ResourceMetrics().RemoveIf(func(rsp pmetric.ResourceMetrics) bool {
				incomingServiceName := getServiceName(rsp.Resource().Attributes())
				rsp.ScopeMetrics().RemoveIf(func(ss pmetric.ScopeMetrics) bool {
					ss.Metrics().RemoveIf(func(lr pmetric.Metric) bool {
						matched := incomingServiceName == sName && lr.Name() == mName && lr.Type().String() == mType
						if matched {
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
				marshalled, me := e.jsonMarshaller.metricsMarshaler.MarshalMetrics(copyObj)
				if me == nil {
					marshalledExemplars = append(marshalledExemplars, &chqpb.MetricExemplar{
						ServiceName: sName,
						MetricName:  mName,
						MetricType:  mType,
						ProcessorId: e.id.Name(),
						Exemplar:    marshalled,
					})
				}
			}
		}

		statsReport := &chqpb.MetricStatsReport{
			SubmittedAt: now.UnixMilli(),
			Stats:       stats,
			Exemplars:   marshalledExemplars,
		}
		// TODO should send this to a channel and have a separate goroutine send it
		go func() {
			err := e.postMetricStats(context.Background(), statsReport)
			if err != nil {
				e.logger.Error("Failed to send metric stats", zap.Error(err))
			}
		}()
	}
	return nil
}

func (e *statsProc) addMetricsExemplar(lm pmetric.Metrics, serviceName, metricName, metricType string) {
	fingerprint := serviceName + ":" + metricName + ":" + metricType
	e.exemplarsMu.Lock()
	defer e.exemplarsMu.Unlock()
	if _, found := e.metricExemplars[fingerprint]; !found {
		e.metricExemplars[fingerprint] = lm
	}
}

func (e *statsProc) postMetricStats(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(e.statsBatchSize, int64(len(b)))
	e.logger.Debug("Sending metric stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Endpoint + "/api/v1/metricstats"
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
