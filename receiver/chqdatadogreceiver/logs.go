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

package datadogreceiver

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

type DDLog struct {
	DDSource string `json:"ddsource,omitempty"`
	DDTags   string `json:"ddtags,omitempty"`
	Message  string `json:"message,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Service  string `json:"service,omitempty"`
}

func handleLogsPayload(req *http.Request) (ddLogs []DDLog, err error) {
	ddLogs = make([]DDLog, 0)
	err = json.NewDecoder(req.Body).Decode(&ddLogs)
	if err != nil {
		return nil, err
	}
	return ddLogs, nil
}

func (ddr *datadogReceiver) processLogs(t pcommon.Timestamp, logs []DDLog) error {
	for _, log := range logs {
		otelLog, err := ddr.convertLog(t, log)
		if err != nil {
			return err
		}
		if err := ddr.nextLogConsumer.ConsumeLogs(context.Background(), otelLog); err != nil {
			return err
		}
	}
	return nil
}

func splitTags(tags string) map[string]string {
	tagMap := make(map[string]string)
	if tags == "" {
		return tagMap
	}
	for _, tag := range strings.Split(tags, ",") {
		kv := strings.Split(tag, ":")
		if len(kv) == 2 && kv[1] != "" && kv[0] != "" {
			tagMap[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return tagMap
}

func (ddr *datadogReceiver) convertLog(t pcommon.Timestamp, log DDLog) (plog.Logs, error) {
	lm := plog.NewLogs()
	rl := lm.ResourceLogs().AppendEmpty()
	rAttr := rl.Resource().Attributes()
	rl.SetSchemaUrl(semconv.SchemaURL)
	rAttr.PutStr(string(semconv.ServiceNameKey), log.Service)
	rAttr.PutStr(string(semconv.HostNameKey), log.Hostname)
	scope := rl.ScopeLogs().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	sAttr.PutStr(string(semconv.TelemetrySDKNameKey), "Datadog")
	logRecord := scope.LogRecords().AppendEmpty()
	logRecord.SetObservedTimestamp(t)
	logRecord.Body().SetStr(log.Message)
	lAttr := logRecord.Attributes()
	if log.DDSource != "" {
		lAttr.PutStr("dd.source", log.DDSource)
	}

	tags := splitTags(log.DDTags)
	severityNumber, severityString := toSeverity(tags["status"])
	logRecord.SetSeverityNumber(severityNumber)
	logRecord.SetSeverityText(severityString)
	delete(tags, "status")
	for k, v := range tags {
		decorate(k, v, rAttr, sAttr)
	}
	return lm, nil
}

func toSeverity(s string) (plog.SeverityNumber, string) {
	s = strings.ToLower(s)
	number := plog.SeverityNumberUnspecified
	switch s {
	case "error":
		number = plog.SeverityNumberError
	case "warn":
		number = plog.SeverityNumberWarn
	case "info":
		number = plog.SeverityNumberInfo
	case "debug":
		number = plog.SeverityNumberDebug
	case "trace":
		number = plog.SeverityNumberTrace
	}
	return number, number.String()
}
