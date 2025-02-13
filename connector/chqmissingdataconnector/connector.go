package chqmissingdataconnector

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

// count can count spans, span event, metrics, data points, or log records
// and emit the counts onto a metrics pipeline.
type md struct {
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc

	entries syncmap.SyncMap[uint64, *Stamp]
}

func (c *md) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

var (
	resourceAttributesToCopy = []string{
		translate.CardinalFieldCustomerID,
	}

	datapointAttributesToCopy = []string{
		translate.CardinalFieldCustomerID,
	}
)

func (c *md) Start(ctx context.Context, host component.Host) error {
	c.logger.Info("Starting")
	return nil
}

func (c *md) Shutdown(ctx context.Context) error {
	c.logger.Info("Shutting down")
	return nil
}

func (c *md) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	countMetrics := pmetric.NewMetrics()
	countMetrics.ResourceMetrics().EnsureCapacity(md.ResourceMetrics().Len())

	now := time.Now()

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)

		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetric.ScopeMetrics().At(j)

			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)

				newResourceAttributes := pcommon.NewMap()
				for _, key := range resourceAttributesToCopy {
					if value, found := resourceMetric.Resource().Attributes().Get(key); found {
						newResourceAttributes.PutStr(key, value.AsString())
					}
				}

				hashkey := makeStampHash(metric.Name(), newResourceAttributes)
				found := c.entries.Touch(hashkey, func(s *Stamp) *Stamp {
					s.Touch(now)
					return s
				})
				if !found {
					c.entries.Store(hashkey, NewStamp(metric.Name(), newResourceAttributes, now))
				}
			}
		}
	}
	return nil
}

func OrgIdFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}
