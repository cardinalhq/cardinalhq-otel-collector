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
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/stats"
)

type beagle struct {
	sync.RWMutex

	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                 component.ID
	ttype              string
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	pbPhase            chqpb.Phase
	podName            string
	vendor             string

	logstats    *stats.StatsCombiner[*chqpb.LogStats]
	spanStats   *stats.StatsCombiner[*chqpb.SpanStats]
	metricstats *stats.StatsCombiner[*MetricStat]
}

func newBeagle(config *Config, ttype string, set processor.Settings) (*beagle, error) {
	now := time.Now()
	dog := &beagle{
		id:                 set.ID,
		ttype:              ttype,
		config:             config,
		httpClientSettings: config.Statistics.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		podName:            os.Getenv("POD_NAME"),
		vendor:             config.Statistics.Vendor,
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

func (e *beagle) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *beagle) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

	return nil
}

func (e *beagle) Shutdown(ctx context.Context) error {
	var errors *multierror.Error
	//e.configExtension.UnregisterCallback(e.configCallbackID)
	return errors.ErrorOrNil()
}

func (e *beagle) processEnrichments(enrichments []StatsEnrichment, attributesByScope map[string]pcommon.Map) map[string]string {
	tags := make(map[string]string)
	for _, enrichment := range enrichments {
		for scope, attributes := range attributesByScope {
			for _, tag := range enrichment.Tags {
				if tagValue, found := attributes.Get(tag); found {
					key := fmt.Sprintf("%s.%s", scope, tag)
					tags[key] = tagValue.AsString()
				}
			}
		}
	}
	return tags
}

func ToMap(attributes pcommon.Map) map[string]string {
	result := make(map[string]string)
	attributes.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}
