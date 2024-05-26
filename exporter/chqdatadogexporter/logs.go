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

	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type DDLog struct {
	DDSource string `json:"ddsource,omitempty"`
	DDTags   string `json:"ddtags,omitempty"`
	Message  string `json:"message,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Service  string `json:"service,omitempty"`
}

func (e *datadogExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	part := 0
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		var ddlogs []DDLog
		rl := logs.ResourceLogs().At(i)
		rAttr := rl.Resource().Attributes()
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			ill := rl.ScopeLogs().At(j)
			sAttr := ill.Scope().Attributes()
			for k := 0; k < ill.LogRecords().Len(); k++ {
				l := ill.LogRecords().At(k)
				lAttr := l.Attributes()
				hostname := "unknown"
				if hostnameField, found := rAttr.Get("host.name"); found {
					hostname = hostnameField.Str()
					rAttr.Remove("host.name")
				}
				serviceName := "unknown"
				if serviceNameField, found := rAttr.Get("service.name"); found {
					serviceName = serviceNameField.Str()
					rAttr.Remove("service.name")
				}
				ddsource := "unknown"
				if ddsourceField, found := lAttr.Get("ddsource"); found {
					ddsource = ddsourceField.Str()
					lAttr.Remove("ddsource")
				}
				ddlog := DDLog{
					Message:  l.Body().Str(),
					Hostname: hostname,
					Service:  serviceName,
					DDSource: ddsource,
					DDTags:   tagString(rAttr, sAttr, lAttr),
				}
				ddlogs = append(ddlogs, ddlog)
			}
		}
		if len(ddlogs) > 0 {
			e.logger.Info("Sending logs", zap.Int("logCount", len(ddlogs)), zap.Int("part", part))
			if err := e.send(ctx, ddlogs); err != nil {
				return err
			}
		}
		part++
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
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return fmt.Errorf("failed to send logs, status code: %d", resp.StatusCode)
	}
	return nil
}
