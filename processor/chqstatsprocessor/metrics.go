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
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

func (p *statsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	ee := authenv.GetEnvironment(ctx, p.idSource)

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		serviceName := getServiceName(rm.Resource().Attributes())
		rattr := rm.Resource().Attributes()
		cid := OrgIdFromResource(rattr)
		tenant := p.getTenant(cid)
		newFingerprintsDetected := make([]string, 0)

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			sattr := ilm.Scope().Attributes()
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)
				metricName := m.Name()
				extra := map[string]string{"name": m.Name()}

				p.addMetricsExemplar(tenant, rm, ilm, m, serviceName, metricName, m.Type(), &newFingerprintsDetected)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						dp := m.Gauge().DataPoints().At(l)
						p.processDatapoint(tenant, ee, metricName, pmetric.MetricTypeGauge.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						dp := m.Sum().DataPoints().At(l)
						p.processDatapoint(tenant, ee, metricName, pmetric.MetricTypeSum.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						dp := m.Histogram().DataPoints().At(l)
						p.processDatapoint(tenant, ee, metricName, pmetric.MetricTypeHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						dp := m.Summary().DataPoints().At(l)
						p.processDatapoint(tenant, ee, metricName, pmetric.MetricTypeSummary.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < m.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						p.processDatapoint(tenant, ee, metricName, pmetric.MetricTypeExponentialHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes())
					}
				}
			}
		}
		if len(newFingerprintsDetected) > 0 {
			p.postExemplars(ee.CustomerID(), ee.CollectorID(), tenant, newFingerprintsDetected)
		}
	}

	return md, nil
}

func (p *statsProcessor) processDatapoint(tenant *Tenant, environment authenv.Environment, metricName, metricType, serviceName string, extra map[string]string, rattr, sattr, dattr pcommon.Map) {
	tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
	if err := p.recordDatapoint(tenant, environment, metricName, metricType, serviceName, tid, rattr, sattr, dattr); err != nil {
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

func (p *statsProcessor) recordDatapoint(tenant *Tenant, environment authenv.Environment, metricName, metricType, serviceName string, tid int64, rattr, sattr, dpAttr pcommon.Map) error {
	orgID := OrgIdFromResource(rattr)

	var errs error

	attributes := p.processEnrichments(orgID,
		map[string]pcommon.Map{
			"resource": rattr,
			"scope":    sattr,
			"metric":   dpAttr,
		})

	rattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(tenant, environment, metricName, metricType, serviceName, k, v.AsString(), "resource", attributes))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(tenant, environment, metricName, metricType, serviceName, k, v.AsString(), "scope", attributes))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(tenant, environment, metricName, metricType, serviceName, k, v.AsString(), "datapoint", attributes))
		}
		return true
	})
	errs = multierr.Append(errs, p.recordMetric(tenant, environment, metricName, metricType, serviceName, translate.CardinalFieldTID, strconv.FormatInt(tid, 10), "metric", attributes))
	return errs
}

func (p *statsProcessor) recordMetric(tenant *Tenant, environment authenv.Environment, metricName, metricType, serviceName, tagName, tagValue, tagScope string, attributes []*chqpb.Attribute) error {
	if !p.enableMetricMetrics {
		return nil
	}
	err := tenant.metricstats.Record(p.pbPhase, metricName, metricType, tagScope, tagName, serviceName, p.id.Name(), environment.CollectorID(), environment.CustomerID(), tagValue, attributes)
	if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
		telemetry.CounterAdd(p.cacheFull, 1)
	}
	return nil
}

func (p *statsProcessor) postExemplars(customerID, collectorID string, tenant *Tenant, fingerprints []string) {
	var marshalledExemplars []*chqpb.MetricExemplar
	for _, fingerprint := range fingerprints {
		split := strings.Split(fingerprint, ":")
		sName := split[0]
		mName := split[1]
		mType := split[2]
		exemplar, found := tenant.metricExemplars.Get(hashString(fingerprint))
		if found {
			exemplarBytes := exemplar.([]byte)
			marshalledExemplars = append(marshalledExemplars, &chqpb.MetricExemplar{
				ServiceName: sName,
				MetricName:  mName,
				MetricType:  mType,
				ProcessorId: p.id.Name(),
				Exemplar:    exemplarBytes,
				CustomerId:  customerID,
				CollectorId: collectorID,
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

func (p *statsProcessor) addMetricsExemplar(tenant *Tenant, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, serviceName, metricName string, metricType pmetric.MetricType, newFingerprints *[]string) {
	if p.pbPhase == chqpb.Phase_PRE {
		fingerprintString := serviceName + ":" + metricName + ":" + metricType.String()
		fingerprint := hashString(fingerprintString)
		if tenant.metricExemplars.Contains(fingerprint) {
			return
		}

		exemplarLm := toExemplar(rm, sm, mm, metricType)

		marshalled, err := p.jsonMarshaller.metricsMarshaler.MarshalMetrics(exemplarLm)
		if err != nil {
			p.logger.Error("Failed to marshal exemplar metric", zap.Error(err))
			return
		}

		tenant.metricExemplars.Put(fingerprint, marshalled)
		*newFingerprints = append(*newFingerprints, fingerprintString)
	}
}

func toExemplar(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricType pmetric.MetricType) pmetric.Metrics {
	exemplarLm := pmetric.NewMetrics()
	copyRm := exemplarLm.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(copyRm.Resource())
	copySm := copyRm.ScopeMetrics().AppendEmpty()
	sm.Scope().CopyTo(copySm.Scope())
	copyMm := copySm.Metrics().AppendEmpty()
	copyMm.SetName(mm.Name())
	copyMm.SetDescription(mm.Description())
	copyMm.SetUnit(mm.Unit())

	switch metricType {
	case pmetric.MetricTypeGauge:
		if mm.Gauge().DataPoints().Len() > 0 {
			newGauge := copyMm.SetEmptyGauge()
			dp := mm.Gauge().DataPoints().At(0)
			ccd := newGauge.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeSum:
		if mm.Sum().DataPoints().Len() > 0 {
			newSum := copyMm.SetEmptySum()
			dp := mm.Sum().DataPoints().At(0)
			ccd := newSum.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeHistogram:
		if mm.Histogram().DataPoints().Len() > 0 {
			newHistogram := copyMm.SetEmptyHistogram()
			dp := mm.Histogram().DataPoints().At(0)
			ccd := newHistogram.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeSummary:
		if mm.Summary().DataPoints().Len() > 0 {
			newSummary := copyMm.SetEmptySummary()
			dp := mm.Summary().DataPoints().At(0)
			ccd := newSummary.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeExponentialHistogram:
		if mm.ExponentialHistogram().DataPoints().Len() > 0 {
			newExponentialHistogram := copyMm.SetEmptyExponentialHistogram()
			dp := mm.ExponentialHistogram().DataPoints().At(0)
			ccd := newExponentialHistogram.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	}
	return exemplarLm
}
