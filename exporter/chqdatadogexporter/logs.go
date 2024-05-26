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

package chqdatadogexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type DDLog struct {
	DDSource string `json:"ddsource,omitempty"`
	DDTags   string `json:"ddtags,omitempty"`
	Message  string `json:"message,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Service  string `json:"service,omitempty"`
}

func getHostname(r pcommon.Map) string {
	if hostnameField, found := r.Get("host.name"); found {
		r.Remove("host.name")
		return hostnameField.Str()
	}
	return "unknown"
}

func getServiceName(r pcommon.Map) string {
	if serviceNameField, found := r.Get("service.name"); found {
		r.Remove("service.name")
		return serviceNameField.Str()
	}
	return "unknown"
}

func getDDSource(l pcommon.Map) string {
	if ddsourceField, found := l.Get("ddsource"); found {
		l.Remove("ddsource")
		return ddsourceField.Str()
	}
	return "unknown"
}

func (e *datadogExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	e.logger.Info("ConsumeLogs", zap.Int("resourceCount", logs.ResourceLogs().Len()))
	var ddlogs []DDLog
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		rAttr := pcommon.NewMap()
		rl.Resource().Attributes().CopyTo(rAttr)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			ill := rl.ScopeLogs().At(j)
			sAttr := pcommon.NewMap()
			ill.Scope().Attributes().CopyTo(sAttr)
			for k := 0; k < ill.LogRecords().Len(); k++ {
				l := ill.LogRecords().At(k)
				lAttr := pcommon.NewMap()
				l.Attributes().CopyTo(lAttr)
				ddlog := DDLog{
					Message:  l.Body().Str(),
					Hostname: getHostname(rAttr),
					Service:  getServiceName(rAttr),
					DDSource: getDDSource(lAttr),
					DDTags:   tagString(rAttr, sAttr, lAttr),
				}
				ddlogs = append(ddlogs, ddlog)
			}
		}
	}
	e.messagesReceived.Add(ctx, int64(len(ddlogs)), metric.WithAttributeSet(e.commonAttributes))
	if len(ddlogs) > 0 {
		e.logger.Info("Sending logs", zap.Int("logCount", len(ddlogs)))
		if err := e.send(ctx, ddlogs); err != nil {
			return err
		}
	}

	return nil
}

func (e *datadogExporter) send(ctx context.Context, ddlogs []DDLog) error {
	b, err := json.Marshal(ddlogs)
	if err != nil {
		return err
	}

	target := fmt.Sprintf("%s/api/v2/logs", e.endpoint)
	req, err := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", e.apiKey)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	e.messagesSubmitted.Add(ctx, int64(len(ddlogs)), metric.WithAttributeSet(e.commonAttributes), metric.WithAttributes(attribute.Int("http.code", resp.StatusCode)))
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return fmt.Errorf("failed to send logs, status code: %d", resp.StatusCode)
	}
	return nil
}
