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
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	rAttrMap = map[string]string{
		"k8s.persistentvolume.access_mode":       "access_mode",
		string(semconv.ContainerIDKey):           "container_id",
		string(semconv.LogFilePathKey):           "dirname",
		string(semconv.DeploymentEnvironmentKey): "env",
		string(semconv.LogFileNameKey):           "filename",
		"k8s.helm.chart.name":                    "helm_chart",
		"k8s.helm.release.name":                  "helm_release",
		"k8s.helm.release.version":               "helm_version",
		"k8s.horizontalpodautoscaler.name":       "horizontalpodautoscaler",
		string(semconv.HostNameKey):              "host",
		string(semconv.ContainerImageNameKey):    "image_name",
		string(semconv.ContainerImageTagsKey):    "image_tag",
		"network.interface.name":                 "interface",
		"host.kernel.version":                    "kernel_version",
		"k8s.app.component":                      "kube_app_component",
		"k8s.app.instance":                       "kube_app_instance",
		"k8s.app.managed_by":                     "kube_app_managed_by",
		"k8s.app.name":                           "kube_app_name",
		"k8s.app.part_of":                        "kube_app_part_of",
		"k8s.app.version":                        "kube_app_version",
		string(semconv.K8SClusterNameKey):        "kube_cluster_name",
		string(semconv.CloudProviderKey):         "kube_cluster",
		string(semconv.ContainerNameKey):         "kube_container_name",
		string(semconv.K8SDaemonSetNameKey):      "kube_daemon_set",
		string(semconv.K8SDeploymentNameKey):     "kube_deployment",
		"k8s.ingress.host":                       "kube_ingress_host",
		"k8s.ingress.path":                       "kube_ingress_path",
		"k8s.ingress.name":                       "kube_ingress",
		string(semconv.K8SJobNameKey):            "kube_job",
		string(semconv.K8SNamespaceNameKey):      "kube_namespace",
		string(semconv.K8SNodeNameKey):           "kube_node",
		"k8s.ownerref.kind":                      "kube_ownerref_kind",
		string(semconv.K8SPodNameKey):            "kube_pod_name",
		string(semconv.K8SPodUIDKey):             "kube_pod_uid",
		"k8s.pod.quality_of_service":             "kube_qos",
		string(semconv.CloudRegionKey):           "kube_region",
		string(semconv.K8SReplicaSetNameKey):     "kube_replica_set",
		"k8s.service.port":                       "kube_service_port",
		"k8s.service.name":                       "kube_service",
		string(semconv.K8SStatefulSetNameKey):    "kube_statefulset",
		string(semconv.CloudAvailabilityZoneKey): "kube_zone",
		"k8s.kubelet.version":                    "kubelet_version",
		string(semconv.HostImageNameKey):         "os_image",
		"k8s.persistentvolume.name":              "persistentvolume",
		"k8s.persistentvolumeclaim.name":         "persistentvolumeclaim",
		"k8s.pod.phase":                          "pod_phase",
		"k8s.poddisruptionbudget.name":           "poddisruptionbudget",
		string(semconv.ContainerRuntimeKey):      "runtime",
		"k8s.secret.name":                        "secret",
		string(semconv.ServiceNameKey):           "service",
		"container.image.short_name":             "short_image",
		"k8s.storageclass.name":                  "storageclass",
		"k8s.verticalpodautoscaler.name":         "verticalpodautoscaler",
	}

	sAttrMap = map[string]string{
		string(semconv.TelemetrySDKLanguageKey): "language",
	}
)

func tagStrings(resourceAttrs pcommon.Map, scopeAttrs pcommon.Map, logAttrs pcommon.Map) (tags []string, resources []string) {
	// This should really go onto the resource list, but it seems datadog doesn't
	// add these to the metric, so we will do this.
	resourceAttrs.Range(func(k string, v pcommon.Value) bool {
		if tag, ok := rAttrMap[k]; ok {
			resources = append(resources, tag+":"+v.AsString())
		}
		return true
	})
	scopeAttrs.Range(func(k string, v pcommon.Value) bool {
		if tag, ok := sAttrMap[k]; ok {
			tags = append(tags, tag+":"+v.AsString())
		}
		return true
	})
	logAttrs.Range(func(k string, v pcommon.Value) bool {
		tags = append(tags, k+":"+v.AsString())
		return true
	})
	return tags, resources
}

func tagString(resourceAttrs pcommon.Map, scopeAttrs pcommon.Map, logAttrs pcommon.Map) string {
	tags, resources := tagStrings(resourceAttrs, scopeAttrs, logAttrs)
	if len(resources) > 0 {
		tags = append(tags, resources...)
	}
	return strings.Join(tags, ",")
}
