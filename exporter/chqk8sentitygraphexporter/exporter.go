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

package chqk8sentitygraphexporter

import (
	"context"
	"net/http"
	"os"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

type exp struct {
	config     *Config
	httpClient *http.Client
	logger     *zap.Logger

	id                 component.ID
	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings

	objecthandler  objecthandler.ObjectHandler
	gee            objecthandler.GraphObjectEmitter
	k8sClusterName string
}

func newEntityGraphExporter(config *Config, set exporter.Settings) (*exp, error) {
	e := &exp{
		id:                 set.ID,
		config:             config,
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  set.TelemetrySettings,
		logger:             set.Logger,
		k8sClusterName:     os.Getenv("K8S_CLUSTER_NAME"),
	}

	return e, nil
}

func (e *exp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *exp) Start(ctx context.Context, host component.Host) error {
	httpClient, err := e.httpClientSettings.ToClient(ctx, host, e.telemetrySettings)
	if err != nil {
		return err
	}
	e.httpClient = httpClient

	glogger, err := zap.NewDevelopment()
	if err != nil {
		glogger = e.logger
	} else {
		glogger = glogger.Named("graph-object-emitter")
	}
	gee, err := objecthandler.NewGraphObjectEmitter(glogger, e.httpClient, e.config.Reporting.Interval, e.config.Endpoint)
	if err != nil {
		return err
	}
	e.gee = gee
	e.gee.Start(ctx)

	cconf := converterconfig.New(e.k8sClusterName).WithHashItems(e.k8sClusterName)
	e.objecthandler = objecthandler.NewObjectHandler(cconf)

	return nil
}

func (e *exp) Shutdown(ctx context.Context) error {
	e.gee.Stop(ctx)
	return nil
}
