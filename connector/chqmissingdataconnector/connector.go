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
	"os"
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
			c.logger.Info("tenant config removed", zap.String("tenant", tid))
		}
	}

	c.logger.Info("config update", zap.Int("tenants", len(sc.Configs)))
	for tid, tc := range sc.Configs {
		keys := []string{}
		for key := range tc.MissingDataConfig {
			keys = append(keys, key)
		}
		c.logger.Info("Looking for my ID", zap.String("ID", c.id.Name()), zap.Any("keys", keys))
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
	c.logger.Info("tenant config loaded", zap.String("tenant", tid), zap.Int("metricAttribute", len(tenant.metricAttributes)), zap.Int("resourceAttributes", len(tenant.resourceAttributes)))
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
	emitList := []*stamp{}
	c.entries.Range(func(key uint64, value *stamp) bool {
		if value.isExpired(now, c.config.MaximumAge) {
			c.entries.Delete(key)
			return true
		}
		emitList = append(emitList, value)
		return true
	})
	return emitList
}

func (c *md) buildMetrics(emitList []*stamp) pmetric.Metrics {
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
			addEnvironmentTags(rm.Resource().Attributes())
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

func (c *md) emitList(emitList []*stamp) {
	c.logger.Info("emitting", zap.Int("count", len(emitList)))

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

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)
		cid := orgIDFromResource(resourceMetric.Resource().Attributes())
		tenant, found := c.tenants.Load(cid)
		if !found {
			continue
		}

		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetric.ScopeMetrics().At(j)

			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
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
					found := c.entries.Touch(hashkey, func(s *stamp) *stamp {
						s.touch(now)
						return s
					})
					if !found {
						c.entries.Store(hashkey, newStamp(metric.Name(), rattrs, dpattr, now))
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

func addEnvironmentTags(attrs pcommon.Map) {
	if podid := os.Getenv("POD_NAME"); podid != "" {
		attrs.PutStr("missingdata.k8s.pod.name", podid)
	}
	if podid := os.Getenv("POD_NAMESPACE"); podid != "" {
		attrs.PutStr("missingdata.k8s.pod.namespace", podid)
	}
	if val := os.Getenv("K8S_NODE_NAME"); val != "" {
		attrs.PutStr("missingdata.k8s.node.name", val)
	}
}

func orgIDFromResource(resource pcommon.Map) string {
	orgID, found := resource.Get(translate.CardinalFieldCustomerID)
	if !found {
		return "default"
	}
	return orgID.AsString()
}
