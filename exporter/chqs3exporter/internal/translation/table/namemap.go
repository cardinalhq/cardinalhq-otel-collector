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

import (
	"fmt"
	"strings"
	"unicode"
)

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

func sanitizeAttribute(s string) string {
	// replace all non-alphanumeric characters with underscores, except for hyphens
	for i := 0; i < len(s); i++ {
		if !isAlphanumeric(s[i]) && s[i] != '-' && s[i] != '.' {
			s = s[:i] + "_" + s[i+1:]
		}
	}

	// remove leading and trailing underscores
	for len(s) > 0 && s[0] == '_' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == '_' {
		s = s[:len(s)-1]
	}

	// replace runs of _ with a single _
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '_' && s[i+1] == '_' {
			s = s[:i] + s[i+1:]
			i--
		}
	}

	return snakecaseLabels(s)
}

func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func snakecaseLabels(s string) string {
	items := strings.Split(s, ".")
	for i, item := range items {
		items[i] = toSnakeCase(item)
	}
	return strings.Join(items, ".")
}

func toSnakeCase(str string) string {
	if is_uppercase(str) {
		return strings.ToLower(str)
	}
	var result []rune
	runes := []rune(str)
	length := len(runes)

	for i := 0; i < length; i++ {
		r := runes[i]
		if unicode.IsUpper(r) {
			// If not the first character and the previous character is not an underscore, add an underscore
			if i > 0 && runes[i-1] != '_' {
				// Check if the next character is also uppercase
				if i+1 < length && unicode.IsUpper(runes[i+1]) {
					// If it's a sequence of uppercase letters, keep them together
					result = append(result, unicode.ToLower(r))
				} else {
					result = append(result, '_', unicode.ToLower(r))
				}
			} else {
				result = append(result, unicode.ToLower(r))
			}
		} else {
			result = append(result, r)
		}
	}

	return string(result)
}

func is_uppercase(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) && !unicode.IsUpper(r) {
			return false
		}
	}
	return true
}
