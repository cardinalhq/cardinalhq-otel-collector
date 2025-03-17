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

package baseobj

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func TestBaseObject_identifier(t *testing.T) {
	tests := []struct {
		name     string
		baseObj  BaseObject
		expected string
	}{
		{
			name: "Namespace is empty",
			baseObj: BaseObject{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "mypod",
				UID:        "12345",
			},
			expected: "v1/Pod/mypod",
		},
		{
			name: "Namespace is not empty",
			baseObj: BaseObject{
				APIVersion: "v1",
				Kind:       "Pod",
				Namespace:  "mynamespace",
				Name:       "mypod",
				UID:        "12345",
			},
			expected: "v1/Pod/mynamespace/mypod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.baseObj.computeIdentifier()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOwnerrefsFromUnstructured(t *testing.T) {
	tests := []struct {
		name     string
		input    unstructured.Unstructured
		expected []OwnerRef
	}{
		{
			name: "Single owner reference with controller",
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetOwnerReferences([]v1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "ReplicaSet",
						Name:       "my-replicaset",
						Controller: ptr.To(true),
					},
				})
				return us
			}(),
			expected: []OwnerRef{
				{
					APIVersion: "v1",
					Kind:       "ReplicaSet",
					Name:       "my-replicaset",
					Controller: true,
				},
			},
		},
		{
			name: "Multiple owner references without controller",
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetOwnerReferences([]v1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "my-deployment",
					},
					{
						APIVersion: "v1",
						Kind:       "Namespace",
						Name:       "my-namespace",
					},
				})
				return us
			}(),
			expected: []OwnerRef{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "my-deployment",
				},
				{
					APIVersion: "v1",
					Kind:       "Namespace",
					Name:       "my-namespace",
				},
			},
		},
		{
			name: "No owner references",
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetOwnerReferences([]v1.OwnerReference{})
				return us
			}(),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ownerrefsFromUnstructured(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilteredAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		config      *converterconfig.Config
		annotations map[string]string
		expected    map[string]string
	}{
		{
			name: "No annotations are filtered",
			config: func() *converterconfig.Config {
				c := converterconfig.New()
				return c
			}(),
			annotations: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name: "Some annotations are filtered",
			config: func() *converterconfig.Config {
				c := converterconfig.New()
				c = c.WithIgnoredAnnotations(mockAnnotationMatcher("key1"))
				return c
			}(),
			annotations: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: map[string]string{
				"key2": "value2",
			},
		},
		{
			name: "All annotations are filtered",
			config: func() *converterconfig.Config {
				c := converterconfig.New()
				c = c.WithIgnoredAnnotations(mockAnnotationMatcher("key1"))
				c = c.WithIgnoredAnnotations(mockAnnotationMatcher("key2"))
				return c
			}(),
			annotations: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: map[string]string{},
		},
		{
			name: "No annotations provided",
			config: func() *converterconfig.Config {
				c := converterconfig.New()
				c = c.WithIgnoredAnnotations(mockAnnotationMatcher("key1"))
				return c
			}(),
			annotations: nil,
			expected:    map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filteredAnnotations(tt.config, tt.annotations)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBaseFromUnstructured(t *testing.T) {
	tests := []struct {
		name     string
		config   *converterconfig.Config
		input    unstructured.Unstructured
		expected BaseObject
	}{
		{
			name: "Basic unstructured object with no annotations or owner references",
			config: func() *converterconfig.Config {
				return converterconfig.New()
			}(),
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetAPIVersion("v1")
				us.SetKind("Pod")
				us.SetName("mypod")
				us.SetNamespace("mynamespace")
				us.SetUID("12345")
				us.SetResourceVersion("67890")
				us.SetLabels(map[string]string{"app": "test"})
				return us
			}(),
			expected: BaseObject{
				APIVersion:      "v1",
				Kind:            "Pod",
				Name:            "mypod",
				Namespace:       "mynamespace",
				UID:             "12345",
				ResourceVersion: "67890",
				Labels:          map[string]string{"app": "test"},
				Annotations:     map[string]string{},
				OwnerRef:        nil,
				ID:              "v1/Pod/mynamespace/mypod",
			},
		},
		{
			name: "Unstructured object with filtered annotations",
			config: func() *converterconfig.Config {
				c := converterconfig.New()
				c = c.WithIgnoredAnnotations(mockAnnotationMatcher("filter-me"))
				return c
			}(),
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetAPIVersion("v1")
				us.SetKind("Pod")
				us.SetName("mypod")
				us.SetNamespace("mynamespace")
				us.SetUID("12345")
				us.SetResourceVersion("67890")
				us.SetAnnotations(map[string]string{
					"filter-me": "value1",
					"keep-me":   "value2",
				})
				return us
			}(),
			expected: BaseObject{
				APIVersion:      "v1",
				Kind:            "Pod",
				Name:            "mypod",
				Namespace:       "mynamespace",
				UID:             "12345",
				ResourceVersion: "67890",
				Labels:          nil,
				Annotations: map[string]string{
					"keep-me": "value2",
				},
				OwnerRef: nil,
				ID:       "v1/Pod/mynamespace/mypod",
			},
		},
		{
			name: "Unstructured object with owner references",
			config: func() *converterconfig.Config {
				return converterconfig.New()
			}(),
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetAPIVersion("v1")
				us.SetKind("Pod")
				us.SetName("mypod")
				us.SetNamespace("mynamespace")
				us.SetUID("12345")
				us.SetResourceVersion("67890")
				us.SetOwnerReferences([]v1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "my-replicaset",
						Controller: ptr.To(true),
					},
				})
				return us
			}(),
			expected: BaseObject{
				APIVersion:      "v1",
				Kind:            "Pod",
				Name:            "mypod",
				Namespace:       "mynamespace",
				UID:             "12345",
				ResourceVersion: "67890",
				Labels:          nil,
				Annotations:     map[string]string{},
				OwnerRef: []OwnerRef{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "my-replicaset",
						Controller: true,
					},
				},
				ID: "v1/Pod/mynamespace/mypod",
			},
		},
		{
			name: "Unstructured object with no namespace",
			config: func() *converterconfig.Config {
				return converterconfig.New()
			}(),
			input: func() unstructured.Unstructured {
				us := unstructured.Unstructured{}
				us.SetAPIVersion("v1")
				us.SetKind("Node")
				us.SetName("mynode")
				us.SetUID("54321")
				us.SetResourceVersion("98765")
				return us
			}(),
			expected: BaseObject{
				APIVersion:      "v1",
				Kind:            "Node",
				Name:            "mynode",
				Namespace:       "",
				UID:             "54321",
				ResourceVersion: "98765",
				Labels:          nil,
				Annotations:     map[string]string{},
				OwnerRef:        nil,
				ID:              "v1/Node/mynode",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BaseFromUnstructured(tt.config, tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBaseObject_GetID(t *testing.T) {
	bo := BaseObject{ID: "v1/Pod/mynamespace/mypod"}
	assert.Equal(t, "v1/Pod/mynamespace/mypod", bo.GetID())

	bo = BaseObject{ID: ""}
	assert.Equal(t, "", bo.GetID())
}

func TestBaseObject_GetResourceVersion(t *testing.T) {
	bo := BaseObject{ResourceVersion: "v1"}
	assert.Equal(t, "v1", bo.GetResourceVersion())

	bo = BaseObject{ResourceVersion: ""}
	assert.Equal(t, "", bo.GetResourceVersion())
}

func TestBaseObject_GetUID(t *testing.T) {
	bo := BaseObject{UID: "12345"}
	assert.Equal(t, "12345", bo.GetUID())

	bo = BaseObject{UID: ""}
	assert.Equal(t, "", bo.GetUID())
}

func mockAnnotationMatcher(matchKey string) converterconfig.StringMatcher {
	return &mockStringMatcher{matchKey}
}

type mockStringMatcher struct {
	matchKey string
}

func (m *mockStringMatcher) Match(s string) bool {
	return s == m.matchKey
}
