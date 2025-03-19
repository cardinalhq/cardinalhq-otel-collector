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
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"github.com/cardinalhq/oteltools/signalbuilder"
)

type md struct {
	id              component.ID
	config          *Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc

	entries     syncmap.SyncMap[uint64, *stamp]
	emitterDone chan struct{}

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

	tenants syncmap.SyncMap[string, *tenantConfig]
}

type tenantConfig struct {
	metricAttributes   map[string][]string
	resourceAttributes map[string][]string
}

func (c *md) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *md) Start(_ context.Context, host component.Host) error {
	// If we are dynamic, set that up here.
	if c.config.ConfigurationExtension != nil {
		c.logger.Info("dynamic configuration enabled")
		if err := c.setupExtension(host); err != nil {
			return err
		}
	} else {
		c.logger.Info("static configuration enabled")
		c.setupStaticConfig()
	}

	go c.emitter()

	return nil
}

func (c *md) setupStaticConfig() {
	mm := []ottl.MissingDataMetric{}
	for _, metric := range c.config.Metrics {
		mm = append(mm, ottl.MissingDataMetric{
			Name:               metric.Name,
			Attributes:         metric.Attributes,
			ResourceAttributes: metric.ResourceAttributes,
		})
	}
	c.buildAttributeMaps("default", mm)
}

func (c *md) setupExtension(host component.Host) error {
	ext, found := host.GetExtensions()[*c.config.ConfigurationExtension]
	if !found {
		return errors.New("extension not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("extension is not a chqconfigextension")
	}
	c.configExtension = cext
	c.configCallbackID = c.configExtension.RegisterCallback(c.id.String()+"/metrics", c.configUpdateCallback)
	c.logger.Info("registered config callback", zap.Int("callbackID", c.configCallbackID))
	return nil
}

func (c *md) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	currentTenants := c.tenants.Keys()
	for _, tid := range currentTenants {
		if _, found := sc.Configs[tid]; !found {
			c.tenants.Delete(tid)
		}
	}

	for tid, tc := range sc.Configs {
		if mdc, found := tc.MissingDataConfig[c.id.Name()]; found {
			c.buildAttributeMaps(tid, mdc.Metrics)
		}
	}
}

func (c *md) buildAttributeMaps(tid string, metrics []ottl.MissingDataMetric) {
	tenant := &tenantConfig{
		metricAttributes:   make(map[string][]string),
		resourceAttributes: make(map[string][]string),
	}
	for _, metric := range metrics {
		tenant.metricAttributes[metric.Name] = metric.Attributes
		tenant.resourceAttributes[metric.Name] = metric.ResourceAttributes
	}
	c.tenants.Store(tid, tenant)
}

func (c *md) Shutdown(_ context.Context) error {
	if c.configExtension != nil {
		c.configExtension.UnregisterCallback(c.configCallbackID)
	}
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
			emitList := c.buildEmitList(now)
			c.emitList(emitList)
		}
	}
}

func (c *md) buildEmitList(now time.Time) []*stamp {
	var emitList []*stamp
	c.entries.RemoveIf(func(key uint64, value *stamp) bool {
		if value.isExpired(now, c.config.MaximumAge) {
			return true
		}
		emitList = append(emitList, value)
		return false
	})
	return emitList
}

func (c *md) buildMetrics(emitList []*stamp) pmetric.Metrics {
	now := time.Now()
	nowTimestamp := pcommon.NewTimestampFromTime(now)
	builder := signalbuilder.NewMetricsBuilder()
	emptymap := pcommon.NewMap()

	for _, stamp := range emitList {
		rm := builder.Resource(stamp.ResourceAttributes)
		sm := rm.Scope(emptymap)
		m, err := sm.Metric(c.config.MetricName, "s", pmetric.MetricTypeGauge)
		if err != nil {
			c.logger.Error("failed to create metric", zap.Error(err))
			continue
		}
		dp, _, _ := m.Datapoint(stamp.DatapointAttributes, nowTimestamp)
		dp.SetDoubleValue(now.Sub(stamp.LastSeen).Seconds())
	}

	return builder.Build()
}

func (c *md) emitList(emitList []*stamp) {
	md := c.buildMetrics(emitList)

	if md.DataPointCount() == 0 {
		return
	}

	if err := c.metricsConsumer.ConsumeMetrics(context.Background(), md); err != nil {
		c.logger.Error("failed to emit metrics", zap.Error(err))
	}
}

func (c *md) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	countMetrics := pmetric.NewMetrics()
	countMetrics.ResourceMetrics().EnsureCapacity(md.ResourceMetrics().Len())

	now := time.Now()

	for i := range md.ResourceMetrics().Len() {
		resourceMetric := md.ResourceMetrics().At(i)
		cid := orgIDFromResource(resourceMetric.Resource().Attributes())
		tenant, found := c.tenants.Load(cid)
		if !found {
			continue
		}

		for j := range resourceMetric.ScopeMetrics().Len() {
			scopeMetrics := resourceMetric.ScopeMetrics().At(j)

			for k := range scopeMetrics.Metrics().Len() {
				metric := scopeMetrics.Metrics().At(k)
				dpAttrsToSelect, found := tenant.metricAttributes[metric.Name()]
				if !found {
					continue
				}

				wantedResourceAttrs := c.config.ResourceAttributesToCopy
				metricResourceAttrs := tenant.resourceAttributes[metric.Name()]
				wantedResourceAttrs = append(wantedResourceAttrs, metricResourceAttrs...)
				rattrs := filteredAttributes(resourceMetric.Resource().Attributes(), wantedResourceAttrs)

				uniqueDatapoints := map[uint64]pcommon.Map{}
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					gauge := metric.Gauge()
					for l := range gauge.DataPoints().Len() {
						dp := gauge.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						c.addMetricNameAttribute(dpattrs, metric.Name())
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeSum:
					sum := metric.Sum()
					for l := range sum.DataPoints().Len() {
						dp := sum.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						c.addMetricNameAttribute(dpattrs, metric.Name())
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeHistogram:
					h := metric.Histogram()
					for l := range h.DataPoints().Len() {
						dp := h.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						c.addMetricNameAttribute(dpattrs, metric.Name())
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeExponentialHistogram:
					eh := metric.ExponentialHistogram()
					for l := range eh.DataPoints().Len() {
						dp := eh.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						c.addMetricNameAttribute(dpattrs, metric.Name())
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeSummary:
					s := metric.Summary()
					for l := range s.DataPoints().Len() {
						dp := s.DataPoints().At(l)
						dpattrs := filteredAttributes(dp.Attributes(), dpAttrsToSelect)
						c.addMetricNameAttribute(dpattrs, metric.Name())
						dphash := hashAttributes(dpattrs)
						uniqueDatapoints[dphash] = dpattrs
					}
				case pmetric.MetricTypeEmpty:
				}

				for _, dpattr := range uniqueDatapoints {
					hashkey := hashMetric(rattrs, dpattr)
					found := c.entries.Touch(hashkey, func(s *stamp) *stamp {
						s.touch(now)
						return s
					})
					if !found {
						c.entries.Store(hashkey, newStamp(rattrs, dpattr, now))
					}
				}
			}
		}
	}
	return nil
}

func (c *md) addMetricNameAttribute(attrs pcommon.Map, metricName string) {
	attrs.PutStr(c.config.MetricNameAttribute, metricName)
	attrs.PutBool(translate.CardinalFieldAggregate, true)
	attrs.PutStr(translate.CardinalFieldAggregationType, ottl.AggregationTypeMin.String())
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

func orgIDFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}
