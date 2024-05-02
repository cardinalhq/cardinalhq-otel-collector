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

package table

import "fmt"

var OtelToDatadogResource = map[string]string{
	"aws.ecs.cluster.arn":     "ecs_cluster_name",
	"aws.ecs.container.arn":   "ecs_container_name",
	"aws.ecs.task.arn":        "task_arn",
	"aws.ecs.task.family":     "task_family",
	"aws.ecs.task.revision":   "task_revision",
	"cloud.availability_zone": "zone",
	"cloud.provider":          "cloud_provider",
	"cloud.region":            "region",
	"container.id":            "container_id",
	"container.image.name":    "image_name",
	"container.image.tag":     "image_tag",
	"container.name":          "container_name",
	"deployment.environment":  "env",
	"k8s.cluster.name":        "kube_cluster_name",
	"k8s.container.name":      "kube_container_name",
	"k8s.cronjob.name":        "kube_cronjob",
	"k8s.daemonset.name":      "kube_daemon_set",
	"k8s.deployment.name":     "kube_deployment",
	"k8s.job.name":            "kube_job",
	"k8s.namespace.name":      "kube_namespace",
	"k8s.pod.name":            "pod_name",
	"k8s.replicaset.name":     "kube_replica_set",
	"k8s.statefulset.name":    "kube_stateful_set",
	"service.name":            "service",
	"service.version":         "version",
}

var OtelToDatadogHostnameSearch = []string{
	"host",
	"datadog.host.name",
	"host.name",
	"host.id",
}

func findHostname(attrs map[string]any) string {
	for _, key := range OtelToDatadogHostnameSearch {
		if val, found := attrs[key]; found {
			s, ok := val.(string)
			if ok {
				return s
			}
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}
