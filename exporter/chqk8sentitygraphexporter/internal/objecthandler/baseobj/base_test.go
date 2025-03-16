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
			expected: "v1/Pod/mypod/12345",
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
			expected: "v1/Pod/mynamespace/mypod/12345",
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

func mockAnnotationMatcher(matchKey string) converterconfig.StringMatcher {
	return &mockStringMatcher{matchKey}
}

type mockStringMatcher struct {
	matchKey string
}

func (m *mockStringMatcher) Match(s string) bool {
	return s == m.matchKey
}
