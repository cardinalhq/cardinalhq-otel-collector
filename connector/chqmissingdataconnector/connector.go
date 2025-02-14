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
		case <-c.emitterDone:
			return
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
		}
	}
}

func (c *md) buildMetrics(emitList []Stamp) pmetric.Metrics {
	now := time.Now()
	md := pmetric.NewMetrics()
	resourceMap := map[uint64]pmetric.ResourceMetrics{}

	for _, stamp := range emitList {
		resourceKey := hashAttributes(stamp.ResourceAttributes)
		rm, exists := resourceMap[resourceKey]
		var sm pmetric.ScopeMetrics
		if exists {
			sm = rm.ScopeMetrics().At(0)
		} else {
			rm = md.ResourceMetrics().AppendEmpty()
			stamp.ResourceAttributes.CopyTo(rm.Resource().Attributes())
			sm = rm.ScopeMetrics().AppendEmpty()
			resourceMap[resourceKey] = rm
		}

		metric := sm.Metrics().AppendEmpty()
		metric.SetName(c.config.MetricName)
		metric.SetDescription("Missing data age in seconds")
		metric.SetUnit("s")

		dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		dp.SetDoubleValue(now.Sub(stamp.LastSeen).Seconds())
		stamp.DatapointAttributes.CopyTo(dp.Attributes())
		dp.Attributes().PutStr(c.config.MetricNameAttribute, stamp.MetricName)
	}

	return md
}

func (c *md) emitList(emitList []Stamp) {
	c.logger.Info("emitting", zap.Int("count", len(emitList)))

	md := c.buildMetrics(emitList)

	if md.DataPointCount() == 0 {
		return
	}

	if err := c.metricsConsumer.ConsumeMetrics(context.Background(), md); err != nil {
		c.logger.Error("failed to emit metrics", zap.Error(err))
	}
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
				dpAttrsToSelect, found := c.config.metricAttributes[metric.Name()]
				if !found {
					continue
				}
				rattrs := filteredAttributes(resourceMetric.Resource().Attributes(), c.config.ResourceAttributesToCopy)

				uniqueDatapoints := map[uint64]pcommon.Map{}
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					gauge := metric.Gauge()
					for l := 0; l < gauge.DataPoints().Len(); l++ {
						dp := gauge.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeSum:
					sum := metric.Sum()
					for l := 0; l < sum.DataPoints().Len(); l++ {
						dp := sum.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeHistogram:
					h := metric.Histogram()
					for l := 0; l < h.DataPoints().Len(); l++ {
						dp := h.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeExponentialHistogram:
					eh := metric.ExponentialHistogram()
					for l := 0; l < eh.DataPoints().Len(); l++ {
						dp := eh.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeSummary:
					s := metric.Summary()
					for l := 0; l < s.DataPoints().Len(); l++ {
						dp := s.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				}

				for _, dpattr := range uniqueDatapoints {
					hashkey := hashMetric(metric.Name(), rattrs, dpattr)
					found := c.entries.Touch(hashkey, func(s *Stamp) *Stamp {
						s.Touch(now)
						return s
					})
					if !found {
						c.entries.Store(hashkey, NewStamp(metric.Name(), rattrs, dpattr, now))
					}
				}
			}
		}
	}
	return nil
}

func filteredAttributes(attrs pcommon.Map, keys []string) pcommon.Map {
	raw := attrs.AsRaw()
	selected := map[string]any{}
	for _, key := range keys {
		if value, found := raw[key]; found {
			selected[key] = value
		}
	}
	ret := pcommon.NewMap()
	_ = ret.FromRaw(selected)
	return ret
}
