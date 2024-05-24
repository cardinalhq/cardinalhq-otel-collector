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
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
)

var (
	rAttrMap = map[string]string{
		"access_mode":             "k8s.persistentvolume.access_mode",
		"container_id":            string(semconv.ContainerIDKey),
		"container_name":          string(semconv.ContainerNameKey),
		"dirname":                 string(semconv.LogFilePathKey),
		"env":                     string(semconv.DeploymentEnvironmentKey),
		"filename":                string(semconv.LogFileNameKey),
		"helm_chart":              "k8s.helm.chart.name",
		"helm_release":            "k8s.helm.release.name",
		"helm_version":            "k8s.helm.release.version",
		"horizontalpodautoscaler": "k8s.horizontalpodautoscaler.name",
		"host":                    string(semconv.HostNameKey),
		"image_name":              string(semconv.ContainerImageNameKey),
		"image_tag":               string(semconv.ContainerImageTagsKey),
		"interface":               "network.interface.name",
		"kernel_version":          "host.kernel.version",
		"kube_app_component":      "k8s.app.component",
		"kube_app_instance":       "k8s.app.instance",
		"kube_app_managed_by":     "k8s.app.managed_by",
		"kube_app_name":           "k8s.app.name",
		"kube_app_part_of":        "k8s.app.part_of",
		"kube_app_version":        "k8s.app.version",
		"kube_cluster_name":       string(semconv.K8SClusterNameKey),
		"kube_cluster":            string(semconv.CloudProviderKey),
		"kube_container_name":     string(semconv.ContainerNameKey),
		"kube_daemon_set":         string(semconv.K8SDaemonSetNameKey),
		"kube_deployment":         string(semconv.K8SDeploymentNameKey),
		"kube_ingress_host":       "k8s.ingress.host",
		"kube_ingress_path":       "k8s.ingress.path",
		"kube_ingress":            "k8s.ingress.name",
		"kube_job":                string(semconv.K8SJobNameKey),
		"kube_namespace":          string(semconv.K8SNamespaceNameKey),
		"kube_node":               string(semconv.K8SNodeNameKey),
		"kube_ownerref_kind":      "k8s.ownerref.kind",
		"kube_pod_name":           string(semconv.K8SPodNameKey),
		"kube_pod_uid":            string(semconv.K8SPodUIDKey),
		"kube_qos":                "k8s.pod.quality_of_service",
		"kube_region":             string(semconv.CloudRegionKey),
		"kube_replica_set":        string(semconv.K8SReplicaSetNameKey),
		"kube_service_port":       "k8s.service.port",
		"kube_service":            "k8s.service.name",
		"kube_statefulset":        string(semconv.K8SStatefulSetNameKey),
		"kube_zone":               string(semconv.CloudAvailabilityZoneKey),
		"kubelet_version":         "k8s.kubelet.version",
		"node":                    string(semconv.K8SNodeNameKey),
		"os_image":                string(semconv.HostImageNameKey),
		"persistentvolume":        "k8s.persistentvolume.name",
		"persistentvolumeclaim":   "k8s.persistentvolumeclaim.name",
		"pod_name":                string(semconv.K8SPodNameKey),
		"pod_phase":               "k8s.pod.phase",
		"poddisruptionbudget":     "k8s.poddisruptionbudget.name",
		"runtime":                 string(semconv.ContainerRuntimeKey),
		"secret":                  "k8s.secret.name",
		"service":                 string(semconv.ServiceNameKey),
		"short_image":             "container.image.short_name",
		"storageclass":            "k8s.storageclass.name",
		"verticalpodautoscaler":   "k8s.verticalpodautoscaler.name",
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
