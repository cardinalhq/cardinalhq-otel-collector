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

type md struct {
	config          *Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc

	entries     syncmap.SyncMap[uint64, *Stamp]
	emitterDone chan struct{}
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
	go c.emitter()
	return nil
}

func (c *md) Shutdown(ctx context.Context) error {
	close(c.emitterDone)
	return nil
}

func (c *md) emitter() {
	for {
		select {
		case <-time.Tick(c.config.Interval):
			now := time.Now()
			emitList := []Stamp{}
			c.entries.Range(func(key uint64, value *Stamp) bool {
				if value.IsExpired(now, c.config.MaximumAge) {
					c.entries.Delete(key)
					return true
				}
				emitList = append(emitList, *value)
				return true
			})
			c.emitList(emitList)
		case <-c.emitterDone:
			return
		}
	}
}

func (c *md) emitList(emitList []Stamp) {
	c.logger.Info("emitting", zap.Int("count", len(emitList)))
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
