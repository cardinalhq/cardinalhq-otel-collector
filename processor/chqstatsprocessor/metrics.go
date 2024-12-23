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
	var ee translate.Environment
	if e.idsFromEnv {
		ee = translate.EnvironmentFromEnv()
	} else {
		ee = translate.EnvironmentFromAuth(ctx)
	}

	newFingerprintsDetected := make([]string, 0)
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
						e.addMetricsExemplar(rm, ilm, m, serviceName, metricName, pmetric.MetricTypeGauge, &newFingerprintsDetected)
						e.processDatapoint(now, ee, metricName, pmetric.MetricTypeGauge.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						e.addMetricsExemplar(rm, ilm, m, serviceName, metricName, pmetric.MetricTypeSum, &newFingerprintsDetected)
						e.processDatapoint(now, ee, metricName, pmetric.MetricTypeSum.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						e.addMetricsExemplar(rm, ilm, m, serviceName, metricName, pmetric.MetricTypeHistogram, &newFingerprintsDetected)
						e.processDatapoint(now, ee, metricName, pmetric.MetricTypeHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						e.addMetricsExemplar(rm, ilm, m, serviceName, metricName, pmetric.MetricTypeSummary, &newFingerprintsDetected)
						e.processDatapoint(now, ee, metricName, pmetric.MetricTypeSummary.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						e.addMetricsExemplar(rm, ilm, m, serviceName, metricName, pmetric.MetricTypeExponentialHistogram, &newFingerprintsDetected)
						dp := m.ExponentialHistogram().DataPoints().At(l)
						e.processDatapoint(now, ee, metricName, pmetric.MetricTypeExponentialHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				}
			}
		}
	}

	if len(newFingerprintsDetected) > 0 {
		e.postExemplars(newFingerprintsDetected)
	}

	return md, nil
}

func (e *statsProc) processDatapoint(now time.Time, environment translate.Environment, metricName, metricType, serviceName string, extra map[string]string, rattr, sattr, dattr pcommon.Map) {
	tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
	if err := e.recordDatapoint(now, environment, metricName, metricType, serviceName, tid, rattr, sattr, dattr); err != nil {
		e.logger.Error("Failed to record datapoint", zap.Error(err))
	}
}

// TODO need to actually use environment here to record stats

func computeStatsOnField(k string) bool {
	if strings.HasPrefix(k, translate.CardinalFieldTID) {
		return true
	}
	return !strings.HasPrefix(k, translate.CardinalFieldPrefixDot)
}

func (e *statsProc) recordDatapoint(now time.Time, environment translate.Environment, metricName, metricType, serviceName string, tid int64, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error

	attributes := e.processEnrichments(map[string]pcommon.Map{
		"resource": rattr,
		"scope":    sattr,
		"metric":   dpAttr,
	})

	rattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(environment, metricName, metricType, serviceName, k, v.AsString(), "resource", attributes))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(environment, metricName, metricType, serviceName, k, v.AsString(), "scope", attributes))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(environment, metricName, metricType, serviceName, k, v.AsString(), "datapoint", attributes))
		}
		return true
	})
	errs = multierr.Append(errs, e.recordMetric(environment, metricName, metricType, serviceName, translate.CardinalFieldTID, strconv.FormatInt(tid, 10), "metric", attributes))
	return errs
}

func (e *statsProc) recordMetric(environment translate.Environment, metricName, metricType, serviceName, tagName, tagValue, tagScope string, attributes []*chqpb.Attribute) error {
	if !e.enableMetricMetrics {
		return nil
	}
	err := e.metricstats.Record(e.pbPhase, metricName, metricType, tagScope, tagName, serviceName, e.id.Name(), environment.CollectorID(), environment.CustomerID(), tagValue, attributes)
	if err != nil {
		return err
	}

	return nil
}

func (e *statsProc) postExemplars(fingerprints []string) {
	e.exemplarsMu.Lock()
	defer e.exemplarsMu.Unlock()

	var marshalledExemplars []*chqpb.MetricExemplar
	for _, fingerprint := range fingerprints {
		split := strings.Split(fingerprint, ":")
		sName := split[0]
		mName := split[1]
		mType := split[2]
		exemplar, found := e.metricExemplars[fingerprint]
		if found {
			marshalled, me := e.jsonMarshaller.metricsMarshaler.MarshalMetrics(exemplar)
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
		SubmittedAt: time.Now().UnixMilli(),
		Exemplars:   marshalledExemplars,
	}
	go func() {
		err := e.sendReport(context.Background(), statsReport)
		if err != nil {
			e.logger.Error("Failed to send metric stats", zap.Error(err))
		}
	}()
}

func (e *statsProc) addMetricsExemplar(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, serviceName, metricName string, metricType pmetric.MetricType, newExemplars *[]string) {
	if e.pbPhase == chqpb.Phase_PRE {
		fingerprint := serviceName + ":" + metricName + ":" + metricType.String()
		e.exemplarsMu.RLock()
		_, found := e.metricExemplars[fingerprint]
		e.exemplarsMu.RUnlock()

		if found {
			return
		}

		e.exemplarsMu.Lock()
		defer e.exemplarsMu.Unlock()

		exemplarLm := pmetric.NewMetrics()
		copyRm := exemplarLm.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(copyRm.Resource())
		copySm := copyRm.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(copySm.Scope())
		copyMm := copySm.Metrics().AppendEmpty()
		mm.CopyTo(copyMm)
		switch metricType {
		case pmetric.MetricTypeGauge:
			ccd := copyMm.Gauge().DataPoints().AppendEmpty()
			mm.Gauge().DataPoints().At(0).CopyTo(ccd)
		case pmetric.MetricTypeSum:
			ccd := copyMm.Sum().DataPoints().AppendEmpty()
			mm.Sum().DataPoints().At(0).CopyTo(ccd)
		case pmetric.MetricTypeHistogram:
			ccd := copyMm.Histogram().DataPoints().AppendEmpty()
			mm.Histogram().DataPoints().At(0).CopyTo(ccd)
		case pmetric.MetricTypeSummary:
			ccd := copyMm.Summary().DataPoints().AppendEmpty()
			mm.Summary().DataPoints().At(0).CopyTo(ccd)
		case pmetric.MetricTypeExponentialHistogram:
			ccd := copyMm.ExponentialHistogram().DataPoints().AppendEmpty()
			mm.ExponentialHistogram().DataPoints().At(0).CopyTo(ccd)
		default:
		}
		e.metricExemplars[fingerprint] = exemplarLm
		*newExemplars = append(*newExemplars, fingerprint)
	}
}

func (e *statsProc) sendMetricStats(wrappers []*chqpb.MetricStatsWrapper) {
	statsList := make([]*chqpb.MetricStats, 0)
	for _, wrapper := range wrappers {
		if wrapper.Dirty {
			slice, err := wrapper.Hll.ToCompactSlice()
			if err != nil {
				continue
			}
			estimate, err := wrapper.GetEstimate()
			if err != nil {
				continue
			}
			wrapper.Stats.CardinalityEstimate = estimate
			wrapper.Stats.Hll = slice
			statsList = append(statsList, wrapper.Stats)
		}
	}
	wrapper := &chqpb.MetricStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Stats:       statsList,
	}
	if len(statsList) > 0 {
		sampleStat := wrapper.GetStats()[0]
		e.logger.Debug("Sending metric stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(statsList)),
			zap.String("customerId", sampleStat.CustomerId), zap.String("collectorId", sampleStat.CollectorId))
		err := e.sendReport(context.Background(), wrapper)
		if err != nil {
			e.logger.Error("Failed to send metric stats", zap.Error(err))
		}
	}
}

func (e *statsProc) sendReport(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(e.statsBatchSize, int64(len(b)))
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
