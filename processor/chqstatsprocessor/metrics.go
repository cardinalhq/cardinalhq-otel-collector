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
	if !p.config.Statistics.Metrics.StatisticsEnabled && !p.config.Statistics.Metrics.ExemplarsEnabled {
		return md, nil
	}

	ee := authenv.GetEnvironment(ctx, p.idSource)

	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		serviceName := getServiceName(rm.Resource().Attributes())
		rattr := rm.Resource().Attributes()
		cid := OrgIdFromResource(rattr)
		collectorId := CollectorIdFromResource(rattr)
		tenant := p.getTenant(cid)

		for j := range rm.ScopeMetrics().Len() {
			ilm := rm.ScopeMetrics().At(j)
			sattr := ilm.Scope().Attributes()
			for k := range ilm.Metrics().Len() {
				m := ilm.Metrics().At(k)
				metricName := m.Name()

				if p.config.Statistics.Metrics.ExemplarsEnabled {
					p.addMetricsExemplar(tenant, rm, ilm, m, serviceName, metricName, m.Type())
				}

				if !p.config.Statistics.Metrics.StatisticsEnabled {
					continue
				}

				extra := map[string]string{"name": m.Name()}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := range m.Gauge().DataPoints().Len() {
						dp := m.Gauge().DataPoints().At(l)
						p.processDatapoint(tenant, cid, collectorId, metricName, pmetric.MetricTypeGauge.String(), serviceName, extra, rattr, sattr, dp.Attributes(), ee)
					}
				case pmetric.MetricTypeSum:
					for l := range m.Sum().DataPoints().Len() {
						dp := m.Sum().DataPoints().At(l)
						p.processDatapoint(tenant, cid, collectorId, metricName, pmetric.MetricTypeSum.String(), serviceName, extra, rattr, sattr, dp.Attributes(), ee)
					}
				case pmetric.MetricTypeHistogram:
					for l := range m.Histogram().DataPoints().Len() {
						dp := m.Histogram().DataPoints().At(l)
						p.processDatapoint(tenant, cid, collectorId, metricName, pmetric.MetricTypeHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes(), ee)
					}
				case pmetric.MetricTypeSummary:
					for l := range m.Summary().DataPoints().Len() {
						dp := m.Summary().DataPoints().At(l)
						p.processDatapoint(tenant, cid, collectorId, metricName, pmetric.MetricTypeSummary.String(), serviceName, extra, rattr, sattr, dp.Attributes(), ee)
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := range m.ExponentialHistogram().DataPoints().Len() {
						dp := m.ExponentialHistogram().DataPoints().At(l)
						p.processDatapoint(tenant, cid, collectorId, metricName, pmetric.MetricTypeExponentialHistogram.String(), serviceName, extra, rattr, sattr, dp.Attributes(), ee)
					}
				case pmetric.MetricTypeEmpty:
					// Do nothing
				default:
					p.logger.Error("Unknown metric type", zap.String("type", m.Type().String()))
				}
			}
		}
	}

	return md, nil
}

func (p *statsProcessor) processDatapoint(tenant *Tenant, customerId, collectorId, metricName, metricType, serviceName string, extra map[string]string, rattr, sattr, dattr pcommon.Map, environment authenv.Environment) {
	tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
	if err := p.recordDatapoint(tenant, customerId, collectorId, metricName, metricType, serviceName, tid, rattr, sattr, dattr); err != nil {
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

func (p *statsProcessor) recordDatapoint(tenant *Tenant, customerId, collectorId, metricName, metricType, serviceName string, tid int64, rattr, sattr, dpAttr pcommon.Map) error {
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
			errs = multierr.Append(errs, p.recordMetric(tenant, customerId, collectorId, metricName, metricType, serviceName, k, v.AsString(), "resource", attributes))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(tenant, customerId, collectorId, metricName, metricType, serviceName, k, v.AsString(), "scope", attributes))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, p.recordMetric(tenant, customerId, collectorId, metricName, metricType, serviceName, k, v.AsString(), "datapoint", attributes))
		}
		return true
	})
	errs = multierr.Append(errs, p.recordMetric(tenant, customerId, collectorId, metricName, metricType, serviceName, translate.CardinalFieldTID, strconv.FormatInt(tid, 10), "metric", attributes))
	return errs
}

func (p *statsProcessor) recordMetric(tenant *Tenant, customerId, collectorId, metricName, metricType, serviceName, tagName, tagValue, tagScope string, attributes []*chqpb.Attribute) error {
	err := tenant.metricstats.Record(p.pbPhase, metricName, metricType, tagScope, tagName, serviceName, p.id.Name(), collectorId, customerId, tagValue, attributes)
	if err != nil && errors.Is(err, chqpb.ErrCacheFull) {
		telemetry.CounterAdd(p.cacheFull, 1)
	}
	return nil
}

func (p *statsProcessor) toMetricExemplarFingerprint(serviceName, metricName, metricType string) int64 {
	return hashString(serviceName + ":" + metricName + ":" + metricType)
}

func (p *statsProcessor) addMetricsExemplar(tenant *Tenant, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, serviceName, metricName string, metricType pmetric.MetricType) {
	if p.pbPhase == chqpb.Phase_PRE {
		fingerprint := p.toMetricExemplarFingerprint(serviceName, metricName, metricType.String())
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
	case pmetric.MetricTypeEmpty:
		// Do nothing
	default:
		// Do nothing
	}
	return exemplarLm
}
