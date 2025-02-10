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

package chqstatsprocessor

import (
	"bytes"
	"context"
	"errors"
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

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (p *statsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	ee := authenv.GetEnvironment(ctx, p.idsFromAuth)

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

				p.addMetricsExemplar(rm, ilm, m, serviceName, metricName, m.Type(), &newFingerprintsDetected)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						p.processDatapoint(ee, metricName, pmetric.MetricTypeGauge.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						p.processDatapoint(ee, metricName, pmetric.MetricTypeSum.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						p.processDatapoint(ee, metricName, pmetric.MetricTypeHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						p.processDatapoint(ee, metricName, pmetric.MetricTypeSummary.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						p.processDatapoint(ee, metricName, pmetric.MetricTypeExponentialHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				}
			}
		}
	}

	if len(newFingerprintsDetected) > 0 {
		p.postExemplars(newFingerprintsDetected)
	}

	return md, nil
}

func (p *statsProcessor) processDatapoint(environment authenv.Environment, metricName, metricType, serviceName string, extra map[string]string, rattr, sattr, dattr pcommon.Map) {
	tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
	if err := p.recordDatapoint(environment, metricName, metricType, serviceName, tid, rattr, sattr, dattr); err != nil {
		p.logger.Error("Failed to record datapoint", zap.Error(err))
	}
}

// TODO need to actually use environment here to record stats

func computeStatsOnField(k string) bool {
	if strings.HasPrefix(k, translate.CardinalFieldTID) {
		return true
	}
	return !strings.HasPrefix(k, translate.CardinalFieldPrefixDot)
}

func (p *statsProcessor) recordDatapoint(environment authenv.Environment, metricName, metricType, serviceName string, tid int64, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error

	attributes := p.processEnrichments(map[string]pcommon.Map{
		"resource": rattr,
		"scope":    sattr,
		"metric":   dpAttr,
	})

	rattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(environment, metricName, metricType, serviceName, k, v.AsString(), "resource", attributes))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(environment, metricName, metricType, serviceName, k, v.AsString(), "scope", attributes))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(environment, metricName, metricType, serviceName, k, v.AsString(), "datapoint", attributes))
		}
		return true
	})
	errs = multierr.Append(errs, p.recordMetric(environment, metricName, metricType, serviceName, translate.CardinalFieldTID, strconv.FormatInt(tid, 10), "metric", attributes))
	return errs
}

func (p *statsProcessor) recordMetric(environment authenv.Environment, metricName, metricType, serviceName, tagName, tagValue, tagScope string, attributes []*chqpb.Attribute) error {
	if !p.enableMetricMetrics {
		return nil
	}
	err := p.metricstats.Record(p.pbPhase, metricName, metricType, tagScope, tagName, serviceName, p.id.Name(), environment.CollectorID(), environment.CustomerID(), tagValue, attributes)
	if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
		telemetry.CounterAdd(p.cacheFull, 1)
	}
	return nil
}

func (p *statsProcessor) postExemplars(fingerprints []string) {
	var marshalledExemplars []*chqpb.MetricExemplar
	for _, fingerprint := range fingerprints {
		split := strings.Split(fingerprint, ":")
		sName := split[0]
		mName := split[1]
		mType := split[2]
		exemplar, found := p.metricExemplars.Get(hashString(fingerprint))
		if found {
			exemplarBytes := exemplar.([]byte)
			marshalledExemplars = append(marshalledExemplars, &chqpb.MetricExemplar{
				ServiceName: sName,
				MetricName:  mName,
				MetricType:  mType,
				ProcessorId: p.id.Name(),
				Exemplar:    exemplarBytes,
			})
		}
	}

	statsReport := &chqpb.MetricStatsReport{
		SubmittedAt: time.Now().UnixMilli(),
		Exemplars:   marshalledExemplars,
	}
	go func() {
		err := p.sendReport(context.Background(), statsReport)
		if err != nil {
			p.logger.Error("Failed to send metric stats", zap.Error(err))
		}
	}()
}

func (p *statsProcessor) addMetricsExemplar(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, serviceName, metricName string, metricType pmetric.MetricType, newFingerprints *[]string) {
	if p.pbPhase == chqpb.Phase_PRE {
		fingerprintString := serviceName + ":" + metricName + ":" + metricType.String()
		fingerprint := hashString(fingerprintString)
		if p.metricExemplars.Contains(fingerprint) {
			return
		}

		exemplarLm := pmetric.NewMetrics()
		copyRm := exemplarLm.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(copyRm.Resource())
		copySm := copyRm.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(copySm.Scope())
		copyMm := copySm.Metrics().AppendEmpty()
		mm.CopyTo(copyMm)
		switch metricType {
		case pmetric.MetricTypeGauge:
			if mm.Gauge().DataPoints().Len() > 0 {
				ccd := copyMm.Gauge().DataPoints().AppendEmpty()
				mm.Gauge().DataPoints().At(0).CopyTo(ccd)
			}
		case pmetric.MetricTypeSum:
			if mm.Sum().DataPoints().Len() > 0 {
				ccd := copyMm.Sum().DataPoints().AppendEmpty()
				mm.Sum().DataPoints().At(0).CopyTo(ccd)
			}
		case pmetric.MetricTypeHistogram:
			if mm.Histogram().DataPoints().Len() > 0 {
				ccd := copyMm.Histogram().DataPoints().AppendEmpty()
				mm.Histogram().DataPoints().At(0).CopyTo(ccd)
			}
		case pmetric.MetricTypeSummary:
			if mm.Summary().DataPoints().Len() > 0 {
				ccd := copyMm.Summary().DataPoints().AppendEmpty()
				mm.Summary().DataPoints().At(0).CopyTo(ccd)
			}
		case pmetric.MetricTypeExponentialHistogram:
			if mm.ExponentialHistogram().DataPoints().Len() > 0 {
				ccd := copyMm.ExponentialHistogram().DataPoints().AppendEmpty()
				mm.ExponentialHistogram().DataPoints().At(0).CopyTo(ccd)
			}
		default:
		}
		marshalled, me := p.jsonMarshaller.metricsMarshaler.MarshalMetrics(exemplarLm)
		if me != nil {
			return
		}
		p.metricExemplars.Put(fingerprint, marshalled)
		*newFingerprints = append(*newFingerprints, fingerprintString)
	}
}

func (p *statsProcessor) sendMetricStats(wrappers []*chqpb.MetricStatsWrapper) {
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
		p.logger.Debug("Sending metric stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(statsList)),
			zap.String("customerId", sampleStat.CustomerId), zap.String("collectorId", sampleStat.CollectorId))
		err := p.sendReport(context.Background(), wrapper)
		if err != nil {
			p.logger.Error("Failed to send metric stats", zap.Error(err))
		}
	}
}

func (p *statsProcessor) sendReport(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	telemetry.HistogramRecord(p.statsBatchSize, int64(len(b)))
	endpoint := p.config.Endpoint + "/api/v1/metricstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		p.logger.Error("Failed to send metric stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
