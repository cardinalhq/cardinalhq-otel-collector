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

package pitbullprocessor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func (e *pitbull) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}

	environment := translate.EnvironmentFromEnv()
	now := time.Now()
	emptySlice := pcommon.NewSlice()

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		serviceName := getServiceName(rm.Resource().Attributes())
		rattr := rm.Resource().Attributes()
		transformCtx := ottlresource.NewTransformContext(rm.Resource(), rm)
		e.metricTransformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
		if _, found := rattr.Get(translate.CardinalFieldDropMarker); found {
			return true
		}

		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			sattr := ilm.Scope().Attributes()
			transformCtx := ottlscope.NewTransformContext(ilm.Scope(), rm.Resource(), rm)
			e.metricTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
			if _, found := sattr.Get(translate.CardinalFieldDropMarker); found {
				return true
			}

			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				metricName := m.Name()
				extra := map[string]string{"name": m.Name()}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
						if _, found := dp.Attributes().Get(translate.CardinalFieldDropMarker); found {
							return true
						}
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
						if e.pbPhase == chqpb.Phase_POST {
							agg := e.aggregate(rm, ilm, m, dp)
							if agg {
								ottl.CounterAdd(e.aggregatedDatapoints, 1, metric.WithAttributes(
									attribute.String("metric_name", metricName),
									attribute.String("service_name", serviceName),
									attribute.String("vendor", e.vendor),
									attribute.String("phase", e.pbPhase.String())))
								return agg
							}
						}
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
						if _, found := dp.Attributes().Get(translate.CardinalFieldDropMarker); found {
							return true
						}
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
						if e.pbPhase == chqpb.Phase_POST {
							agg := e.aggregate(rm, ilm, m, dp)
							if agg {
								ottl.CounterAdd(e.aggregatedDatapoints, 1, metric.WithAttributes(
									attribute.String("metric_name", metricName),
									attribute.String("service_name", serviceName),
									attribute.String("vendor", e.vendor),
									attribute.String("phase", e.pbPhase.String())))
								return agg
							}
						}
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
						if _, found := dp.Attributes().Get(translate.CardinalFieldDropMarker); found {
							return true
						}
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
						if _, found := dp.Attributes().Get(translate.CardinalFieldDropMarker); found {
							return true
						}
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
						e.processDatapoint(now, metricName, serviceName, rattr, sattr, dp.Attributes())
						return false
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						transformCtx := ottldatapoint.NewTransformContext(dp, m, ilm.Metrics(), ilm.Scope(), rm.Resource(), ilm, rm)
						e.metricTransformations.ExecuteDatapointTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
						if _, found := dp.Attributes().Get(translate.CardinalFieldDropMarker); found {
							return true
						}
						dattr := dp.Attributes()
						tid := translate.CalculateTID(extra, rattr, sattr, dattr, "metric", environment)
						dattr.PutInt(translate.CardinalFieldTID, tid)
						dattr.PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
						dattr.PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())
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

func (e *pitbull) processDatapoint(now time.Time, metricName, serviceName string, rattr, sattr, dattr pcommon.Map) {
	if err := e.recordDatapoint(now, metricName, serviceName, rattr, sattr, dattr); err != nil {
		e.logger.Error("Failed to record datapoint", zap.Error(err))
	}
	if e.config.DropDecorationAttributes {
		removeAllCardinalFields(dattr)
	}
}

func computeStatsOnField(k string) bool {
	if strings.HasPrefix(k, translate.CardinalFieldTID) {
		return true
	}
	return !strings.HasPrefix(k, translate.CardinalFieldPrefixDot)
}

func (e *pitbull) recordDatapoint(now time.Time, metricName, serviceName string, rattr, sattr, dpAttr pcommon.Map) error {
	var errs error

	tags := e.processEnrichments(e.config.MetricsConfig.StatsEnrichments, map[string]pcommon.Map{
		"resource": rattr,
		"scope":    sattr,
		"metric":   dpAttr,
	})
	rattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "resource."+k, v.AsString(), tags, 1))
		}
		return true
	})
	sattr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "scope."+k, v.AsString(), tags, 1))
		}
		return true
	})
	dpAttr.Range(func(k string, v pcommon.Value) bool {
		if computeStatsOnField(k) {
			errs = multierr.Append(errs, e.recordMetric(now, metricName, serviceName, "metric."+k, v.AsString(), tags, 1))
		}
		return true
	})
	return errs
}

func (e *pitbull) recordMetric(now time.Time, metricName, serviceName, tagName, tagValue string, tags map[string]string, count int) error {
	rec := &MetricStat{
		MetricName:  metricName,
		TagName:     tagName,
		ServiceName: serviceName,
		Phase:       e.pbPhase,
		VendorID:    e.config.Statistics.Vendor,
		Count:       int64(count),
		Tags:        tags,
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

func (e *pitbull) sendMetricStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*MetricStat) {
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
			}
			wrapper.Stats = append(wrapper.Stats, item)
		}
	}

	if err := e.postMetricStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send metric stats", zap.Error(err))
	}
	e.logger.Info("Sent metric stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *pitbull) postMetricStats(ctx context.Context, wrapper *chqpb.MetricStatsReport) error {
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

func (c *pitbull) updateMetricTransformation(sc ottl.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating metrics transformations", zap.Int("num_decorators", len(sc.Metrics.Decorators)))
	c.logger.Info("Metrics decorators", zap.Any("decorators", sc.Metrics.Decorators))
	newTransformations := ottl.NewTransformations(c.logger)

	for _, decorator := range sc.Metrics.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing metrics transformation", zap.Error(err))
		} else {
			newTransformations = ottl.MergeWith(newTransformations, transformations)
		}
	}

	oldTransformation := c.metricTransformations
	c.metricTransformations = newTransformations
	oldTransformation.Stop()
}
