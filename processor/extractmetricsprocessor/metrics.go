package extractmetricsprocessor

import (
	"context"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
	"math"
	"time"
)

func orgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}

func (p *extractor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.DataPointCount() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}

	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		cid := orgIdFromResource(rm.Resource().Attributes())

		for j := range rm.ScopeMetrics().Len() {
			ilm := rm.ScopeMetrics().At(j)
			p.updateMetricSketchCache(ctx, cid, rm, ilm, ilm.Metrics())
		}
	}
	return md, nil
}

func (p *extractor) updateMetricSketchCache(
	ctx context.Context,
	cid string,
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	ms pmetric.MetricSlice) {
	metricExtractors, mok := p.metricExtractors.Load(cid)
	if !mok {
		return
	}
	resource := rm.Resource()

	sketchCache, sok := p.metricSketchCaches.Load(cid)
	if !sok {
		p.logger.Info("Creating new metrics sketch cache", zap.String("cid", cid))
		sketchCache = chqpb.NewGenericSketchCache(5*time.Minute, cid, "metrics", func(list *chqpb.GenericSketchList) error {
			send := p.sendProto("/api/v1/metricSketches", list)
			return send()
		})
		p.metricSketchCaches.Store(cid, sketchCache)
	}

	for k := range ms.Len() {
		mm := ms.At(k)
		metricType := mm.Type()
		metricName := mm.Name()
		mex, mexOk := metricExtractors[metricName]
		if !mexOk {
			continue
		}

		switch metricType {
		case pmetric.MetricTypeGauge:
			gaugeDataPoints := mm.Gauge().DataPoints()
			if gaugeDataPoints.Len() > 0 {
				for i := 0; i < gaugeDataPoints.Len(); i++ {
					dp := gaugeDataPoints.At(i)
					tc := ottldatapoint.NewTransformContext(dp, mm, ms, sm.Scope(), resource, sm, rm)
					p.updateWithDataPoint(ctx, dp.DoubleValue(), dp.Timestamp().AsTime(), tc, resource, mex, sketchCache)
				}
			}

		case pmetric.MetricTypeSum:
			sumDataPoints := mm.Sum().DataPoints()
			if sumDataPoints.Len() > 0 {
				for i := 0; i < sumDataPoints.Len(); i++ {
					dp := sumDataPoints.At(i)
					tc := ottldatapoint.NewTransformContext(dp, mm, ms, sm.Scope(), resource, sm, rm)
					p.updateWithDataPoint(ctx, dp.DoubleValue(), dp.Timestamp().AsTime(), tc, resource, mex, sketchCache)
				}
			}

		case pmetric.MetricTypeHistogram:
			histogramDataPoints := mm.Histogram().DataPoints()
			if histogramDataPoints.Len() > 0 {
				for i := 0; i < histogramDataPoints.Len(); i++ {
					dp := histogramDataPoints.At(i)
					if dp.Count() == 0 {
						continue
					}
					p.updateHistogramWithBuckets(ctx, dp, mm, ms, sm, resource, rm, mex, sketchCache)
				}
			}

		case pmetric.MetricTypeSummary:
			summaryDataPoints := mm.Summary().DataPoints()
			if summaryDataPoints.Len() > 0 {
				for i := 0; i < summaryDataPoints.Len(); i++ {
					dp := summaryDataPoints.At(i)
					if dp.Count() == 0 {
						continue
					}
					avgValue := dp.Sum() / float64(dp.Count())
					tc := ottldatapoint.NewTransformContext(dp, mm, ms, sm.Scope(), resource, sm, rm)
					p.updateWithDataPoint(ctx, avgValue, dp.Timestamp().AsTime(), tc, resource, mex, sketchCache)
				}
			}

		case pmetric.MetricTypeExponentialHistogram:
			histogramDataPoints := mm.ExponentialHistogram().DataPoints()
			for i := 0; i < histogramDataPoints.Len(); i++ {
				dp := histogramDataPoints.At(i)
				if dp.Count() == 0 {
					continue
				}

				tc := ottldatapoint.NewTransformContext(dp, mm, ms, sm.Scope(), resource, sm, rm)
				ts := dp.Timestamp().AsTime()
				scale := dp.Scale()

				// Midpoint approximation function
				approxValue := func(index int) float64 {
					base := math.Ldexp(1, int(scale)) // 2^scale
					return math.Exp2(float64(index)*math.Pow(2, float64(-scale))) * base
				}

				updateBuckets := func(buckets pmetric.ExponentialHistogramDataPointBuckets, negate bool) {
					offset := buckets.Offset()
					for j := 0; j < buckets.BucketCounts().Len(); j++ {
						count := buckets.BucketCounts().At(j)
						if count == 0 {
							continue
						}
						index := int(offset) + j
						value := approxValue(index)
						if negate {
							value = -value
						}

						var parentTID int64 = 0
						if len(mex.AggregateDimensions) > 0 {
							mapAttrs := mex.ExtractAggregateAttributes(ctx, tc)
							tags := p.withServiceClusterNamespace(resource, mapAttrs)
							parentTID = sketchCache.UpdateWithCount(mex.MetricName, mex.MetricType, tags, 0, 0, value, count, ts)
						}
						if len(mex.LineDimensions) > 0 {
							mapAttrsByTagFamilyId := mex.ExtractLineAttributes(ctx, tc)
							for tagFamilyId, mapAttrs := range mapAttrsByTagFamilyId {
								tags := p.withServiceClusterNamespace(resource, mapAttrs)
								sketchCache.UpdateWithCount(mex.MetricName, mex.MetricType, tags, parentTID, tagFamilyId, value, count, ts)
							}
						}
					}
				}

				// Update non-zero buckets
				updateBuckets(dp.Positive(), false)
				updateBuckets(dp.Negative(), true)
			}

		case pmetric.MetricTypeEmpty:
			// do nothing
		default:
		}
	}
}

func (p *extractor) updateHistogramWithBuckets(
	ctx context.Context,
	dp pmetric.HistogramDataPoint,
	mm pmetric.Metric,
	ms pmetric.MetricSlice,
	sm pmetric.ScopeMetrics,
	resource pcommon.Resource,
	rm pmetric.ResourceMetrics,
	mex *ottl.MetricSketchExtractor,
	sketchCache *chqpb.GenericSketchCache,
) {
	tc := ottldatapoint.NewTransformContext(dp, mm, ms, sm.Scope(), resource, sm, rm)
	timestamp := dp.Timestamp().AsTime()

	counts := dp.BucketCounts()
	bounds := dp.ExplicitBounds()

	getMidpoint := func(i int) float64 {
		switch {
		case i == 0:
			return bounds.At(0) / 2

		case i == bounds.Len():
			return bounds.At(bounds.Len()-1) + 1

		default:
			return (bounds.At(i-1) + bounds.At(i)) / 2
		}
	}

	for i := 0; i < counts.Len(); i++ {
		bucketCount := counts.At(i)
		if bucketCount == 0 {
			continue
		}
		midpoint := getMidpoint(i)

		var parentTID int64 = 0
		if len(mex.AggregateDimensions) > 0 {
			mapAttrs := mex.ExtractAggregateAttributes(ctx, tc)
			tags := p.withServiceClusterNamespace(resource, mapAttrs)
			parentTID = sketchCache.UpdateWithCount(mex.MetricName, mex.MetricType, tags, 0, 0, midpoint, bucketCount, timestamp)
		}
		if len(mex.LineDimensions) > 0 {
			mapAttrsByTagFamilyId := mex.ExtractLineAttributes(ctx, tc)
			for tagFamilyId, mapAttrs := range mapAttrsByTagFamilyId {
				tags := p.withServiceClusterNamespace(resource, mapAttrs)
				sketchCache.UpdateWithCount(mex.MetricName, mex.MetricType, tags, parentTID, tagFamilyId, midpoint, bucketCount, timestamp)
			}

		}
	}
}

func (p *extractor) updateWithDataPoint(ctx context.Context,
	metricValue float64,
	t time.Time,
	tc ottldatapoint.TransformContext,
	resource pcommon.Resource,
	mex *ottl.MetricSketchExtractor,
	sketchCache *chqpb.GenericSketchCache) {

	var parentTID int64 = 0
	if len(mex.AggregateDimensions) > 0 {
		mapAttrs := mex.ExtractAggregateAttributes(ctx, tc)
		tags := p.withServiceClusterNamespace(resource, mapAttrs)
		parentTID = sketchCache.Update(mex.MetricName, mex.MetricType, tags, 0, 0, metricValue, t)
	}
	if len(mex.LineDimensions) > 0 {
		mapAttrsByTagFamilyId := mex.ExtractLineAttributes(ctx, tc)
		for tagFamilyId, mapAttrs := range mapAttrsByTagFamilyId {
			tags := p.withServiceClusterNamespace(resource, mapAttrs)
			sketchCache.Update(mex.MetricName, mex.MetricType, tags, parentTID, tagFamilyId, metricValue, t)
		}
	}
}
