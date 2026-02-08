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

package chqsyntheticsextention

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/ottl"
	"github.com/cardinalhq/oteltools/pkg/syncmap"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqsyntheticsextention/internal/metadata"
)

type CHQSyntheticsExtension struct {
	id                 component.ID
	config             *Config
	httpClient         *http.Client
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	logger             *zap.Logger

	configExtension  *chqconfigextension.CHQConfigExtension
	configCallbackID int

	queryHistogram telemetry.DeferrableHistogram[int64]

	// Ruchir TODO: add this type to the oteltools package
	tenants        syncmap.SyncMap[string, *ottl.SyntheticPollingConfig]
	tenantChannels syncmap.SyncMap[string, chan *ottl.SyntheticPollingConfig]
}

func (chq *CHQSyntheticsExtension) setupTelemetry(set extension.Settings) error {
	if seh, err := telemetry.NewDeferrableHistogram(metadata.Meter(set.TelemetrySettings),
		"synthetics.pollings",
		[]metric.Int64HistogramOption{
			metric.WithDescription("The timing and results of pollings executed."),
			metric.WithUnit("ns"),
		}, nil); err == nil {
		chq.queryHistogram = seh
	}
	return nil
}

func newExtension(cfg *Config, set extension.Settings) (*CHQSyntheticsExtension, error) {
	chq := CHQSyntheticsExtension{
		id:                 set.ID,
		config:             cfg,
		httpClientSettings: cfg.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
	}
	if err := chq.setupTelemetry(set); err != nil {
		return nil, err
	}
	return &chq, nil
}

func (e *CHQSyntheticsExtension) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host.GetExtensions(), e.telemetrySettings)
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
	e.configCallbackID = e.configExtension.RegisterCallback(e.id.String(), e.configUpdateCallback)

	return nil
}

func (e *CHQSyntheticsExtension) Shutdown(context.Context) error {
	if e.configExtension != nil {
		e.configExtension.UnregisterCallback(e.configCallbackID)
	}
	return nil
}

func (e *CHQSyntheticsExtension) configUpdateCallback(sc ottl.ControlPlaneConfig) {
	e.logger.Info("Configuration updated for extension instance", zap.String("instance", e.id.Name()))
	for cid, tenant := range sc.Configs {
		tc := tenant.SyntheticPollings[e.id.Name()]
		if tc == nil {
			e.stopTenant(cid)
			continue
		}
		e.tenants.Store(cid, tc)
		e.updateTenant(cid, tc)
	}
}

func (e *CHQSyntheticsExtension) stopTenant(cid string) {
	if ch, ok := e.tenantChannels.Load(cid); ok {
		close(ch)
		e.tenantChannels.Delete(cid)
	}
}

func (e *CHQSyntheticsExtension) updateTenant(cid string, tc *ottl.SyntheticPollingConfig) {
	if ch, ok := e.tenantChannels.Load(cid); ok {
		close(ch)
	}
	ch := make(chan *ottl.SyntheticPollingConfig)
	e.tenantChannels.Store(cid, ch)
	go e.pollTenant(cid, tc, ch)
}

func (e *CHQSyntheticsExtension) pollTenant(cid string, tc *ottl.SyntheticPollingConfig, ch chan *ottl.SyntheticPollingConfig) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// loop until the channel is closed.  If we get a new config, remember it.  Otherwise, check to see
	// if we need to run our poll loop.
	for {
		select {
		case newConfig, ok := <-ch:
			if !ok {
				return
			}
			tc = newConfig
			normalizeConfig(tc)
		case <-ticker.C:
			e.pollTargets(cid, e.httpClient, tc)
		}
	}
}

const (
	minTimeout  = 5 * time.Second
	minInterval = 10 * time.Second
)

func normalizeConfig(tc *ottl.SyntheticPollingConfig) {
	if tc.Interval < minInterval {
		tc.Interval = minInterval
	}
	if tc.Timeout < minTimeout {
		tc.Timeout = minTimeout
	}
	for _, t := range tc.Targets {
		if t.Timeout == 0 {
			t.Timeout = tc.Timeout
		}
		if t.Timeout < minTimeout {
			t.Timeout = minTimeout
		}
		if t.Method == "" {
			t.Method = "GET"
		}
	}
}

func (e *CHQSyntheticsExtension) pollTargets(cid string, client *http.Client, tc *ottl.SyntheticPollingConfig) {
	for _, t := range tc.Targets {
		e.pollTarget(cid, client, t)
	}
}

func (e *CHQSyntheticsExtension) pollTarget(cid string, client *http.Client, t ottl.SyntheticPollingTarget) {
	if t.Endpoint == "" {
		return
	}
	attrset := attribute.NewSet(attribute.String("tenant", cid), attribute.String("target", t.Endpoint))
	req, err := http.NewRequest(t.Method, t.Endpoint, nil)
	if err != nil {
		telemetry.HistogramRecord(e.queryHistogram, 0, metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String("result", "error"), attribute.String("error", err.Error())))
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.Timeout)
	defer cancel()
	req = req.WithContext(ctx)
	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		telemetry.HistogramRecord(e.queryHistogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
			metric.WithAttributes(attribute.String("result", "error"), attribute.String("error", err.Error())))
		return
	}
	telemetry.HistogramRecord(e.queryHistogram, time.Since(startTime).Nanoseconds(), metric.WithAttributeSet(attrset),
		metric.WithAttributes(attribute.String("result", "success"), attribute.Int("httpStatus", resp.StatusCode)))
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}
