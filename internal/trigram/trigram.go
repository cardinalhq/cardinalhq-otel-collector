// Copyright 2024-2025 CardinalHQ, Inc
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

package trigram

var (
	IngrastructureTags = []string{
		"container_name",
		"docker_container_id",
		"host",
		"kube_container_name",
		"kube_deployment",
		"kube_namespace",
		"kube_pod_name",
		"service",
		"short_image",
	}

	TagsToIndex = []string{
		"_telemetry_type",
		"aggregation",
		"fieldName",
		"level",
		"message",
		"name",
		"resource",
		"span_id",
		"span_kind",
		"span_name",
		"trace_id",
	}

	FieldExistsMarker = ".*"
)

// ToTrigrams returns a list of trigrams for a string and appends a
// special marker to indicate "exists" for a given field.
func ToTrigrams(s string) []string {
	trigrams := []string{}
	for i := 0; i < len(s)-2; i++ {
		trigrams = append(trigrams, s[i:i+3])
	}
	trigrams = append(trigrams, FieldExistsMarker)
	return trigrams
}
