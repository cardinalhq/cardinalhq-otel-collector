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
	"errors"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type DDLog struct {
	DDSource string `json:"ddsource,omitempty"`
	DDTags   string `json:"ddtags,omitempty"`
	Message  string `json:"message,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Service  string `json:"service,omitempty"`
}

func handleLogsPayload(req *http.Request) (ret []*DDLog, err error) {
	ret = make([]*DDLog, 0)
	defer func() {
		_, errs := io.Copy(io.Discard, req.Body)
		err = errors.Join(err, errs, req.Body.Close())
	}()

	var ddLogs []*DDLog
	err = json.NewDecoder(req.Body).Decode(&ddLogs)
	if err != nil {
		return nil, err
	}
	return ddLogs, nil
}

func (ddr *datadogReceiver) processLogs(t pcommon.Timestamp, logs []*DDLog) error {
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

func (ddr *datadogReceiver) convertLog(t pcommon.Timestamp, log *DDLog) (plog.Logs, error) {
	lm := plog.NewLogs()
	rl := lm.ResourceLogs().AppendEmpty()
	rAttr := rl.Resource().Attributes()
	rAttr.PutStr(string(semconv.ServiceNameKey), log.Service)
	rAttr.PutStr(string(semconv.HostNameKey), log.Hostname)
	scope := rl.ScopeLogs().AppendEmpty()
	sAttr := scope.Scope().Attributes()
	logRecord := scope.LogRecords().AppendEmpty()
	logRecord.SetObservedTimestamp(t)
	logRecord.Body().SetStr(log.Message)
	lAttr := logRecord.Attributes()
	lAttr.PutStr("dd.source", log.DDSource)

	tags := splitTags(log.DDTags)
	if v, ok := tags["status"]; ok {
		v = strings.ToLower(v)
		switch v {
		case "error":
			logRecord.SetSeverityNumber(plog.SeverityNumberError)
		case "warn":
			logRecord.SetSeverityNumber(plog.SeverityNumberWarn)
		case "info":
			logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
		case "debug":
			logRecord.SetSeverityNumber(plog.SeverityNumberDebug)
		case "trace":
			logRecord.SetSeverityNumber(plog.SeverityNumberTrace)
		default:
			logRecord.SetSeverityNumber(plog.SeverityNumberUnspecified)
		}
		logRecord.SetSeverityText(v)
		delete(tags, "status")
	}
	for k, v := range tags {
		decorate(k, v, rAttr, sAttr, lAttr)
	}
	return lm, nil
}

func decorate(k, v string, rAttr, sAttr, lAttr pcommon.Map) {
	switch k {
	case "env":
		rAttr.PutStr(string(semconv.DeploymentEnvironmentKey), v)
	case "language":
		sAttr.PutStr(string(semconv.TelemetrySDKLanguageKey), v)
	case "image_name":
		rAttr.PutStr(string(semconv.ContainerImageNameKey), v)
	case "image_tag":
		rAttr.PutStr(string(semconv.ContainerImageTagKey), v)
	case "pod_name":
		rAttr.PutStr(string(semconv.K8SPodNameKey), v)
	case "kube_deployment":
		rAttr.PutStr(string(semconv.K8SDeploymentNameKey), v)
	case "kube_qos":
		rAttr.PutStr("dd.kube_qos", v)
	case "pod_phase":
		rAttr.PutStr("dd.pod_phase", v)
	case "kube_namespace":
		rAttr.PutStr(string(semconv.K8SNamespaceNameKey), v)
	case "kube_replica_set":
		rAttr.PutStr(string(semconv.K8SReplicasetNameKey), v)
	case "kube_ownerref_kind":
		rAttr.PutStr("dd.kube_ownerref_kind", v)
	case "kube_service":
		rAttr.PutStr("dd.kube_service", v)
	case "short_image":
		rAttr.PutStr("dd.short_image", v)
	case "kube_container_name":
		rAttr.PutStr(string(semconv.ContainerNameKey), v)
	case "contianer_id":
		rAttr.PutStr(string(semconv.ContainerIDKey), v)
	default:
		rAttr.PutStr("dd."+k, v)
	}
}
