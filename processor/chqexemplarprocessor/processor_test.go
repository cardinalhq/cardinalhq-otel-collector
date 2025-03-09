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

package chqexemplarprocessor

import (
	"testing"

	"github.com/cardinalhq/oteltools/pkg/translate"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestGetFromResource(t *testing.T) {
	tests := []struct {
		name     string
		attr     pcommon.Map
		key      string
		expected string
	}{
		{
			name:     "KeyExists",
			attr:     createAttributes(map[string]string{"testKey": "testValue"}),
			key:      "testKey",
			expected: "testValue",
		},
		{
			name:     "KeyDoesNotExist",
			attr:     createAttributes(map[string]string{"anotherKey": "anotherValue"}),
			key:      "testKey",
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFromResource(tt.attr, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOrgIdFromResource(t *testing.T) {
	tests := []struct {
		name     string
		attr     pcommon.Map
		expected string
	}{
		{
			name:     "OrgIDExists",
			attr:     createAttributes(map[string]string{translate.CardinalFieldCustomerID: "org123"}),
			expected: "org123",
		},
		{
			name:     "OrgIDDoesNotExist",
			attr:     createAttributes(map[string]string{"anotherKey": "anotherValue"}),
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := orgIdFromResource(tt.attr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeExemplarKey(t *testing.T) {
	tests := []struct {
		name      string
		rl        pcommon.Resource
		extraKeys []string
		expected  []string
	}{
		{
			name: "WithExtraKeys",
			rl: createResource(map[string]string{
				serviceNameKey:   "serviceA",
				namespaceNameKey: "namespaceA",
				clusterNameKey:   "clusterA",
			}),
			extraKeys: []string{"extraKey1", "extraKey2"},
			expected: []string{
				clusterNameKey, "clusterA",
				namespaceNameKey, "namespaceA",
				serviceNameKey, "serviceA",
				"extraKey1", "extraKey2",
			},
		},
		{
			name: "WithoutExtraKeys",
			rl: createResource(map[string]string{
				serviceNameKey:   "serviceB",
				namespaceNameKey: "namespaceB",
				clusterNameKey:   "clusterB",
			}),
			extraKeys: []string{},
			expected: []string{
				clusterNameKey, "clusterB",
				namespaceNameKey, "namespaceB",
				serviceNameKey, "serviceB",
			},
		},
		{
			name: "MissingAttributes",
			rl: createResource(map[string]string{
				serviceNameKey: "serviceC",
			}),
			extraKeys: []string{"extraKey3"},
			expected: []string{
				clusterNameKey, "unknown",
				namespaceNameKey, "unknown",
				serviceNameKey, "serviceC",
				"extraKey3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, _ := computeExemplarKey(tt.rl, tt.extraKeys)
			assert.Equal(t, tt.expected, keys)
		})
	}
}

func createResource(attrs map[string]string) pcommon.Resource {
	resource := pcommon.NewResource()
	attrMap := resource.Attributes()
	for k, v := range attrs {
		attrMap.PutStr(k, v)
	}
	return resource
}

func createAttributes(attrs map[string]string) pcommon.Map {
	attrMap := pcommon.NewMap()
	for k, v := range attrs {
		attrMap.PutStr(k, v)
	}
	return attrMap
}
