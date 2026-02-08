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
	"time"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func TestHostIPs(t *testing.T) {
	tests := []struct {
		name     string
		pod      corev1.Pod
		expected []string
	}{
		{
			name: "Single host IP",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					HostIPs: []corev1.HostIP{
						{IP: "192.168.1.1"},
					},
				},
			},
			expected: []string{"192.168.1.1"},
		},
		{
			name: "Multiple host IPs",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					HostIPs: []corev1.HostIP{
						{IP: "192.168.1.1"},
						{IP: "10.0.0.1"},
					},
				},
			},
			expected: []string{"192.168.1.1", "10.0.0.1"},
		},
		{
			name: "No host IPs",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					HostIPs: []corev1.HostIP{},
				},
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hostIPs(tt.pod)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestPodIPs(t *testing.T) {
	tests := []struct {
		name     string
		pod      corev1.Pod
		expected []string
	}{
		{
			name: "Single pod IP",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					PodIPs: []corev1.PodIP{
						{IP: "192.168.1.1"},
					},
				},
			},
			expected: []string{"192.168.1.1"},
		},
		{
			name: "Multiple pod IPs",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					PodIPs: []corev1.PodIP{
						{IP: "192.168.1.1"},
						{IP: "10.0.0.1"},
					},
				},
			},
			expected: []string{"192.168.1.1", "10.0.0.1"},
		},
		{
			name: "No pod IPs",
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					PodIPs: []corev1.PodIP{},
				},
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := podIPs(tt.pod)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestConvertPod(t *testing.T) {
	tests := []struct {
		name     string
		input    unstructured.Unstructured
		expected *graphpb.PodSummary
		wantErr  bool
	}{
		{
			name: "Valid Pod with all fields",
			input: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]any{
						"name":      "test-pod",
						"namespace": "default",
					},
					"spec": map[string]any{
						"serviceAccountName": "test-service-account",
						"containers": []any{
							map[string]any{
								"name":  "test-container",
								"image": "test-image",
							},
						},
					},
					"status": map[string]any{
						"phase":     "Running",
						"message":   "Pod is running",
						"startTime": "2023-01-01T00:00:00Z",
						"hostIPs": []any{
							map[string]any{"ip": "192.168.1.1"},
						},
						"podIPs": []any{
							map[string]any{"ip": "10.0.0.1"},
						},
						"containerStatuses": []map[string]any{
							{
								"name":  "test-container",
								"image": "test-image",
							},
						},
					},
				},
			},
			expected: &graphpb.PodSummary{
				BaseObject: &graphpb.BaseObject{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: &graphpb.PodSpec{
					ServiceAccountName: "test-service-account",
					Containers: []*graphpb.PodContainerSpec{
						{
							Name:  "test-container",
							Image: "test-image",
						},
					},
				},
				Status: &graphpb.PodStatus{
					Phase:        "Running",
					PhaseMessage: "Pod is running",
					StartedAt: func() *timestamppb.Timestamp {
						t := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
						return timestamppb.New(t)
					}(),
					ContainerStatus: []*graphpb.PodContainerStatus{
						{
							Name:  "test-container",
							Image: &graphpb.ImageSummary{Image: "test-image"},
						},
					},
					HostIps: []string{"192.168.1.1"},
					PodIps:  []string{"10.0.0.1"},
				},
			},
			wantErr: false,
		},
		{
			name: "Pod with missing optional fields",
			input: unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata": map[string]any{
						"name":      "test-pod",
						"namespace": "default",
					},
					"spec": map[string]any{
						"containers": []any{
							map[string]any{
								"name":  "test-container",
								"image": "test-image",
							},
						},
					},
					"status": map[string]any{
						"phase": "Pending",
						"containerStatuses": []map[string]any{
							{
								"name":  "test-container",
								"image": "test-image",
							},
						},
					},
				},
			},
			expected: &graphpb.PodSummary{
				Spec: &graphpb.PodSpec{
					Containers: []*graphpb.PodContainerSpec{
						{
							Name:  "test-container",
							Image: "test-image",
						},
					},
				},
				Status: &graphpb.PodStatus{
					Phase: "Pending",
					ContainerStatus: []*graphpb.PodContainerStatus{
						{
							Name:  "test-container",
							Image: &graphpb.ImageSummary{Image: "test-image"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid unstructured input",
			input: unstructured.Unstructured{
				Object: map[string]any{
					"invalidField": "invalidValue",
				},
			},
			expected: nil,
			wantErr:  true,
		},
	}

	cconf := &converterconfig.Config{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertPod(cconf, tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.expected.BaseObject = baseobj.Make(cconf, &tt.input, tt.input.GetAPIVersion(), tt.input.GetKind())
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
