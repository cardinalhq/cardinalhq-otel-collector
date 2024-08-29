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

func sanitizeAttribute(input string) string {
	// Split by periods
	parts := strings.Split(input, ".")

	for i, part := range parts {
		// replace all non-alphanumeric characters, dashes, or underscores with underscores
		part = strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
				return r
			}
			return '_'
		}, part)

		// Group substrings based on whether the character is uppercase or not
		groups := groupByCase(part)

		// Process the groups to handle transitions between uppercase and lowercase
		groups = processGroups(groups)

		// Convert to lowercase and join with underscores
		parts[i] = strings.Join(groups, "_")
	}

	// Join the parts back together with periods
	ret := strings.Join(parts, ".")

	// replace multiple underscores with a single underscore
	ret = strings.ReplaceAll(ret, "__", "_")

	// remove leading and trailing underscores
	ret = strings.Trim(ret, "_")

	return strings.ToLower(ret)
}

func groupByCase(input string) []string {
	var groups []string
	currentGroup := strings.Builder{}

	for i, r := range input {
		if i > 0 && unicode.IsUpper(r) != unicode.IsUpper(rune(input[i-1])) {
			// Start a new group when the case changes
			groups = append(groups, currentGroup.String())
			currentGroup.Reset()
		}
		currentGroup.WriteRune(r)
	}

	// Append the last group
	groups = append(groups, currentGroup.String())

	return groups
}

func processGroups(groups []string) []string {
	for i := len(groups) - 1; i > 0; i-- {
		// Check if the last character in the current group is uppercase
		if len(groups[i-1]) > 0 && unicode.IsUpper(rune(groups[i-1][len(groups[i-1])-1])) {
			// Move the last character of the previous group to the start of the current group
			groups[i] = string(groups[i-1][len(groups[i-1])-1]) + groups[i]
			groups[i-1] = groups[i-1][:len(groups[i-1])-1]
		}
	}

	// Remove any empty groups
	var result []string
	for _, group := range groups {
		if len(group) > 0 {
			result = append(result, strings.ToLower(group))
		}
	}

	return result
}
