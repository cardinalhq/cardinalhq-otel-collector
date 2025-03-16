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

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func TestCalculateConfigMapDataHashes(t *testing.T) {
	tests := []struct {
		name      string
		configMap corev1.ConfigMap
		expected  map[string]string
	}{
		{
			name: "ConfigMap with data",
			configMap: corev1.ConfigMap{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "ConfigMap",
				},
				Data: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
			expected: map[string]string{
				"key1": "ya6Q0N5tqMXXKm7CjIvjg0F2ZFeFychM5soeXkLwV0h",
				"key2": "s3bbmDQ5FFNDQgti7PNtiMvbTo6gfOYKGWvyr9tnFAa",
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

	conf := &converterconfig.Config{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateConfigMapDataHashes(conf.HashItems, tt.configMap)
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
			name: "Valid ConfigMap with data and binaryData",
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
					"binaryData": map[string][]byte{
						"key3": []byte("value3"),
					},
				},
			},
			expected: &ConfigMapSummary{
				DataHashes: map[string]string{
					"key1": "eYd97hG7rARdMGKc5IjCbjUNE0w0EXANHAVqy4ZVHv",
					"key2": "iibpTb4YLxEyH2wOFRryq98jNDsf4qmV1pTajSij4ru",
					"key3": "fgcyqHgr1wK5nogXKC9B4Vlkx9qN8VpiqaV0sRpvsyL",
				},
			},
			expectError: false,
		},
		{
			name: "Valid ConfigMap with a different name but same data",
			unstructured: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name":      "test-configmap-2",
						"namespace": "default",
					},
					"data": map[string]any{
						"key1": "value1",
						"key2": "value2",
					},
					"binaryData": map[string][]byte{
						"key3": []byte("value3"),
					},
				},
			},
			expected: &ConfigMapSummary{
				DataHashes: map[string]string{
					"key1": "hsOrHqIlgamTqKUbnUjdYOSSP7HE8nz8qvKCbiMzeof",
					"key2": "TWDQYhFAkA33uunaT868Yju65OJgYjdNffmjA296BmW",
					"key3": "8xqytWmoQjY8LOodULFPszmHk3vlpWdsYqK0I1LVK12",
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
		conf := &converterconfig.Config{}
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertConfigMap(conf, tt.unstructured)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.expected.BaseObject = baseobj.BaseFromUnstructured(conf, tt.unstructured)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIgnoreConfigMapName(t *testing.T) {
	tests := []struct {
		name       string
		config     *converterconfig.Config
		configName string
		expected   bool
	}{
		{
			name: "Name matches one of the ignored names",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: []converterconfig.StringMatcher{
					converterconfig.NewLiteralMatcher("ignored-configmap"),
				},
			},
			configName: "ignored-configmap",
			expected:   true,
		},
		{
			name: "Name does not match any ignored names",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: []converterconfig.StringMatcher{
					converterconfig.NewLiteralMatcher("ignored-configmap"),
				},
			},
			configName: "non-ignored-configmap",
			expected:   false,
		},
		{
			name: "No ignored names configured",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: []converterconfig.StringMatcher{},
			},
			configName: "any-configmap",
			expected:   false,
		},
		{
			name: "Nil ignored names configured",
			config: &converterconfig.Config{
				IgnoredConfigMapNames: nil,
			},
			configName: "any-configmap",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ignoreConfigMapName(tt.config, tt.configName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
