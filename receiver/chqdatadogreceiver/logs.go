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
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
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
	rl.SetSchemaUrl(semconv.SchemaURL)
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

var (
	rAttrMap = map[string]string{
		"container_id":            string(semconv.ContainerIDKey),
		"container_name":          string(semconv.ContainerNameKey),
		"env":                     string(semconv.DeploymentEnvironmentKey),
		"helm_chart":              "k8s.helm.chart.name",
		"helm_release":            "k8s.helm.release.name",
		"helm_version":            "k8s.helm.release.version",
		"horizontalpodautoscaler": "k8s.horizontalpodautoscaler.name",
		"image_name":              string(semconv.ContainerImageNameKey),
		"image_tag":               string(semconv.ContainerImageTagsKey),
		"kube_app_component":      "k8s.app.component",
		"kube_app_instance":       "k8s.app.instance",
		"kube_app_managed_by":     "k8s.app.managed_by",
		"kube_app_name":           "k8s.app.name",
		"kube_app_part_of":        "k8s.app.part_of",
		"kube_app_version":        "k8s.app.version",
		"kube_cluster":            string(semconv.CloudProviderKey),
		"kube_container_name":     string(semconv.ContainerNameKey),
		"kube_daemonset":          string(semconv.K8SDaemonSetNameKey),
		"kube_deployment":         string(semconv.K8SDeploymentNameKey),
		"kube_job":                string(semconv.K8SJobNameKey),
		"kube_namespace":          string(semconv.K8SNamespaceNameKey),
		"kube_node":               string(semconv.K8SNodeNameKey),
		"kube_ownerref_kind":      "k8s.ownerref.kind",
		"kube_pod_name":           string(semconv.K8SPodNameKey),
		"kube_pod_uid":            string(semconv.K8SPodUIDKey),
		"kube_qos":                "k8s.pod.quality_of_service",
		"kube_region":             string(semconv.CloudRegionKey),
		"kube_replica_set":        string(semconv.K8SReplicaSetNameKey),
		"kube_service":            "k8s.service.name",
		"kube_statefulset":        string(semconv.K8SStatefulSetNameKey),
		"kube_zone":               string(semconv.CloudAvailabilityZoneKey),
		"pod_name":                string(semconv.K8SPodNameKey),
		"pod_phase":               "k8s.pod.phase",
		"short_image":             "container.image.short_name",
		"verticalpodautoscaler":   "k8s.verticalpodautoscaler.name",
		"kube_service_port":       "k8s.service.port",
		"kube_ingress_path":       "k8s.ingress.path",
		"kube_ingress":            "k8s.ingress.name",
		"kube_ingress_host":       "k8s.ingress.host",
		"os_image":                string(semconv.HostImageNameKey),
		"kernel_version":          "host.kernel.version",
		"kubelet_version":         "k8s.kubelet.version",
		"node":                    string(semconv.K8SNodeNameKey),
		"poddisruptionbudget":     "k8s.poddisruptionbudget.name",
		"persistentvolume":        "k8s.persistentvolume.name",
		"persistentvolumeclaim":   "k8s.persistentvolumeclaim.name",
		"storageclass":            "k8s.storageclass.name",
		"access_mode":             "k8s.persistentvolume.access_mode",
		"secret":                  "k8s.secret.name",
		"filename":                string(semconv.LogFileNameKey),
		"dirname":                 string(semconv.LogFilePathKey),
	}

	sAttrMap = map[string]string{
		"language": string(semconv.TelemetrySDKLanguageKey),
	}
)

func decorate(k, v string, rAttr pcommon.Map, sAttr pcommon.Map) {
	rmap, ok := rAttrMap[k]
	if ok {
		rAttr.PutStr(rmap, v)
		return
	}

	smap, ok := sAttrMap[k]
	if ok {
		sAttr.PutStr(smap, v)
		return
	}

	rAttr.PutStr("dd."+k, v)
}
