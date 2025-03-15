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

package v1

import (
	"testing"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestCalculateConfigMapDataHashes(t *testing.T) {
	tests := []struct {
		name      string
		configMap corev1.ConfigMap
		expected  map[string]uint64
	}{
		{
			name: "ConfigMap with data",
			configMap: corev1.ConfigMap{
				Data: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
			expected: map[string]uint64{
				"key1": xxhash.Sum64String("value1"),
				"key2": xxhash.Sum64String("value2"),
			},
		},
		{
			name: "Empty ConfigMap data",
			configMap: corev1.ConfigMap{
				Data: map[string]string{},
			},
			expected: nil,
		},
		{
			name: "Nil ConfigMap data",
			configMap: corev1.ConfigMap{
				Data: nil,
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateConfigMapDataHashes(tt.configMap)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertConfigMap(t *testing.T) {
	tests := []struct {
		name         string
		unstructured unstructured.Unstructured
		expected     *ConfigMapSummary
		expectError  bool
	}{
		{
			name: "Valid ConfigMap",
			unstructured: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":      "test-configmap",
						"namespace": "default",
					},
					"data": map[string]any{
						"key1": "value1",
						"key2": "value2",
					},
				},
			},
			expected: &ConfigMapSummary{
				DataHashes: map[string]uint64{
					"key1": xxhash.Sum64String("value1"),
					"key2": xxhash.Sum64String("value2"),
				},
			},
			expectError: false,
		},
		{
			name: "Invalid kind",
			unstructured: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]any{
						"name":      "test-pod",
						"namespace": "default",
					},
				},
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "Invalid API version",
			unstructured: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v2",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":      "test-configmap",
						"namespace": "default",
					},
				},
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "Empty data",
			unstructured: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":      "empty-configmap",
						"namespace": "default",
					},
					"data": map[string]any{},
				},
			},
			expected: &ConfigMapSummary{
				DataHashes: nil,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertConfigMap(tt.unstructured)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.expected.BaseObject = baseobj.BaseFromUnstructured(tt.unstructured)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
