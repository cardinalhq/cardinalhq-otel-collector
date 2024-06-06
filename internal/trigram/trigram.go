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

// FieldTprint returns a hashcode for a field name and trigram
func FieldTprint(name string, trigram string) int64 {
	return JavaHashcode(name + ":" + trigram)
}

// JavaHashcode returns a hashcode for a string.  This is a 64-bit
// implementation of the Java String hashcode function.
func JavaHashcode(str string) int64 {
	var h int64
	length := len(str)
	i := 0

	for ; i+3 < length; i += 4 {
		h = 31*31*31*31*h +
			31*31*31*int64(str[i]) +
			31*31*int64(str[i+1]) +
			31*int64(str[i+2]) +
			int64(str[i+3])
	}

	for ; i < length; i++ {
		h = 31*h + int64(str[i])
	}

	return h
}
