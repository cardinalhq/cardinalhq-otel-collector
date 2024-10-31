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

package chqstatsprocessor

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

type statsProc struct {
	config          *Config
	httpClient      *http.Client
	logger          *zap.Logger
	configExtension *chqconfigextension.CHQConfigExtension

	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	pbPhase            chqpb.Phase
	podName            string
	vendor             string

	configCallbackID int

	logstats    *stats.StatsCombiner[*chqpb.LogStats]
	spanStats   *stats.StatsCombiner[*chqpb.SpanStats]
	metricstats *stats.StatsCombiner[*MetricStat]

	logStatsEnrichments     atomic.Pointer[[]ottl.StatsEnrichment]
	metricsStatsEnrichments atomic.Pointer[[]ottl.StatsEnrichment]
	tracesStatsEnrichments  atomic.Pointer[[]ottl.StatsEnrichment]
}

func newStatsProc(config *Config, ttype string, set processor.Settings) (*statsProc, error) {
	now := time.Now()
	dog := &statsProc{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.Statistics.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		podName:            os.Getenv("POD_NAME"),
	}

	if config.Statistics.Phase == "presample" {
		dog.pbPhase = chqpb.Phase_PRE
	} else {
		dog.pbPhase = chqpb.Phase_POST
	}

	switch ttype {
	case "logs":
		dog.logstats = stats.NewStatsCombiner[*chqpb.LogStats](now, config.Statistics.Interval)
		dog.logger.Info("sending log statistics", zap.Duration("interval", config.Statistics.Interval))
	case "metrics":
		dog.metricstats = stats.NewStatsCombiner[*MetricStat](now, config.Statistics.Interval)
		dog.logger.Info("sending metric statistics", zap.Duration("interval", config.Statistics.Interval))
	case "traces":
		dog.spanStats = stats.NewStatsCombiner[*chqpb.SpanStats](now, config.Statistics.Interval)
		dog.logger.Info("sending span statistics", zap.Duration("interval", config.Statistics.Interval))
	}

	return dog, nil
}

func (e *statsProc) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *statsProc) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

	ext, found := host.GetExtensions()[*e.config.ConfigurationExtension]
	if !found {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " not found")
	}
	cext, ok := ext.(*chqconfigextension.CHQConfigExtension)
	if !ok {
		return errors.New("configuration extension " + e.config.ConfigurationExtension.String() + " is not a chqconfig extension")
	}
	e.configExtension = cext
	e.configCallbackID = e.configExtension.RegisterCallback(e.id.String()+"/"+e.ttype, e.configUpdateCallback)

	return nil
}

func (e *statsProc) Shutdown(ctx context.Context) error {
	var errors *multierror.Error
	e.configExtension.UnregisterCallback(e.configCallbackID)
	return errors.ErrorOrNil()
}

func (e *statsProc) processEnrichments(attributesByScope map[string]pcommon.Map) []*chqpb.Attribute {
	tags := make([]*chqpb.Attribute, 0)
	var enrichments *[]ottl.StatsEnrichment
	switch e.ttype {
	case "logs":
		enrichments = e.logStatsEnrichments.Load()
	case "metrics":
		enrichments = e.metricsStatsEnrichments.Load()
	case "traces":
		enrichments = e.tracesStatsEnrichments.Load()
	}

	if enrichments != nil {
		for _, enrichment := range *enrichments {
			attributes, found := attributesByScope[enrichment.Context]
			if found {
				for _, tag := range enrichment.Tags {
					if tagValue, found := attributes.Get(tag); found {
						tags = append(tags, toAttribute(enrichment.Context, tag, tagValue, false))
					}
				}
			}
		}
	}
	return tags
}

func toAttribute(contextId string, k string, v pcommon.Value, isAttribute bool) *chqpb.Attribute {
	return &chqpb.Attribute{
		ContextId:   contextId,
		IsAttribute: isAttribute,
		Type:        int32(v.Type()),
		Key:         k,
		Value:       v.AsString(),
	}
}

func ToAttributes(resource pcommon.Resource, scope pcommon.InstrumentationScope, recordAttrs pcommon.Map) []*chqpb.Attribute {
	// make an empty slice  that can be appended
	attributes := make([]*chqpb.Attribute, 0)

	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		attributes = append(attributes, toAttribute("resource", k, v, true))
		return true
	})

	scope.Attributes().Range(func(k string, v pcommon.Value) bool {
		attributes = append(attributes, toAttribute("scope", k, v, true))
		return true
	})

	recordAttrs.Range(func(k string, v pcommon.Value) bool {
		attributes = append(attributes, toAttribute("log", k, v, true))
		return true
	})

	return attributes
}

func (e *statsProc) configUpdateCallback(cpc ottl.ControlPlaneConfig) {
	configs := cpc.Stats[e.id.Name()]
	switch e.ttype {
	case "logs":
		e.logStatsEnrichments.Store(&configs.LogEnrichments)
		e.logger.Info("Stats enrichment for logs", zap.String("instance", e.id.Name()), zap.Int("enrichments", len(configs.LogEnrichments)))
	case "metrics":
		e.metricsStatsEnrichments.Store(&configs.MetricEnrichments)
		e.logger.Info("Stats enrichment for metrics", zap.String("instance", e.id.Name()), zap.Int("enrichments", len(configs.MetricEnrichments)))
	case "traces":
		e.tracesStatsEnrichments.Store(&configs.SpanEnrichments)
		e.logger.Info("Stats enrichment for traces", zap.String("instance", e.id.Name()), zap.Int("enrichments", len(configs.SpanEnrichments)))
	}

	e.logger.Info("Configuration updated for processor instance", zap.String("instance", e.id.Name()))
}
