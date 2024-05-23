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
	"compress/gzip"
	"context"
	"encoding/json"
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

	var from io.ReadCloser = req.Body
	defer req.Body.Close()
	if req.Header.Get("Content-Encoding") == "gzip" {
		from, err = gzip.NewReader(req.Body)
		if err != nil {
			return nil, err
		}
		defer from.Close()
	}

	var ddLogs []*DDLog
	err = json.NewDecoder(from).Decode(&ddLogs)
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
	switch s {
	case "error":
		return plog.SeverityNumberError, s
	case "warn":
		return plog.SeverityNumberWarn, s
	case "info":
		return plog.SeverityNumberInfo, s
	case "debug":
		return plog.SeverityNumberDebug, s
	case "trace":
		return plog.SeverityNumberTrace, s
	default:
		return plog.SeverityNumberUnspecified, s
	}
}

func decorate(k, v string, rAttr pcommon.Map, sAttr pcommon.Map) {
	switch k {
	case "container_id":
		rAttr.PutStr(string(semconv.ContainerIDKey), v)
	case "container_name":
		rAttr.PutStr(string(semconv.ContainerNameKey), v)
	case "env":
		rAttr.PutStr(string(semconv.DeploymentEnvironmentKey), v)
	case "image_name":
		rAttr.PutStr(string(semconv.ContainerImageNameKey), v)
	case "image_tag":
		rAttr.PutStr(string(semconv.ContainerImageTagKey), v)
	case "kube_container_name":
		rAttr.PutStr(string(semconv.ContainerNameKey), v)
	case "kube_deployment":
		rAttr.PutStr(string(semconv.K8SDeploymentNameKey), v)
	case "kube_namespace":
		rAttr.PutStr(string(semconv.K8SNamespaceNameKey), v)
	case "kube_ownerref_kind":
		rAttr.PutStr("dd.kube_ownerref_kind", v)
	case "kube_qos":
		rAttr.PutStr("k8s.pod.quality_of_service", v)
	case "kube_replica_set":
		rAttr.PutStr(string(semconv.K8SReplicasetNameKey), v)
	case "kube_statefulset":
		rAttr.PutStr(string(semconv.K8SStatefulsetNameKey), v)
	case "kube_daemonset":
		rAttr.PutStr(string(semconv.K8SDaemonsetNameKey), v)
	case "kube_job":
		rAttr.PutStr(string(semconv.K8SJobNameKey), v)
	case "kube_node":
		rAttr.PutStr(string(semconv.K8SNodeNameKey), v)
	case "kube_pod_uid":
		rAttr.PutStr(string(semconv.K8SPodUIDKey), v)
	case "kube_service":
		rAttr.PutStr("dd.kube_service", v)
	case "kube_pod_name":
		rAttr.PutStr(string(semconv.K8SPodNameKey), v)
	case "language":
		sAttr.PutStr(string(semconv.TelemetrySDKLanguageKey), v)
	case "pod_phase":
		rAttr.PutStr("k8s.pod.phase", v)
	case "short_image":
		rAttr.PutStr("container.image.short_name", v)
	case "horizontalpodautoscaler":
		rAttr.PutStr("k8s.hpa.name", v)
	case "verticalpodautoscaler":
		rAttr.PutStr("k8s.vpa.name", v)
	case "kube_app_name":
		rAttr.PutStr("k8s.app.name", v)
	case "kube_app_instance":
		rAttr.PutStr("k8s.app.instance", v)
	case "kube_app_version":
		rAttr.PutStr("k8s.app.version", v)
	case "kube_app_component":
		rAttr.PutStr("k8s.app.component", v)
	case "kube_app_part_of":
		rAttr.PutStr("k8s.app.part_of", v)
	case "kube_app_managed_by":
		rAttr.PutStr("k8s.app.managed_by", v)
	case "helm_chart":
		rAttr.PutStr("k8s.helm.chart", v)
	case "kube_region":
		rAttr.PutStr(string(semconv.CloudRegionKey), v)
	case "kube_zone":
		rAttr.PutStr(string(semconv.CloudAvailabilityZoneKey), v)
	case "kube_cluster":
		rAttr.PutStr(string(semconv.CloudProviderKey), v)
	default:
		rAttr.PutStr("dd_translated."+k, v)
	}
}
