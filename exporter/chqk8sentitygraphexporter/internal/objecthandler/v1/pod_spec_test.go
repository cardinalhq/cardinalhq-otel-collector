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

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestConvertPodResources(t *testing.T) {
	tests := []struct {
		name     string
		input    corev1.ResourceRequirements
		expected map[string]string
	}{
		{
			name: "No resources",
			input: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{},
				Limits:   corev1.ResourceList{},
			},
			expected: nil,
		},
		{
			name: "Only requests",
			input: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
				Limits: corev1.ResourceList{},
			},
			expected: map[string]string{
				"requests.cpu":    "500m",
				"requests.memory": "128Mi",
			},
		},
		{
			name: "Only limits",
			input: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				},
			},
			expected: map[string]string{
				"limits.cpu":    "1",
				"limits.memory": "256Mi",
			},
		},
		{
			name: "Both requests and limits",
			input: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("250m"),
					corev1.ResourceMemory: resource.MustParse("64Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
			expected: map[string]string{
				"requests.cpu":    "250m",
				"requests.memory": "64Mi",
				"limits.cpu":      "500m",
				"limits.memory":   "128Mi",
			},
		},
		{
			name: "Zero values are ignored",
			input: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("0"),
					corev1.ResourceMemory: resource.MustParse("0"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("0"),
					corev1.ResourceMemory: resource.MustParse("0"),
				},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertPodResources(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContainerSecretNamesFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		container corev1.Container
		expected  []string
	}{
		{
			name: "No secrets in env",
			container: corev1.Container{
				Env:     []corev1.EnvVar{},
				EnvFrom: []corev1.EnvFromSource{},
			},
			expected: []string{},
		},
		{
			name: "Secrets in Env",
			container: corev1.Container{
				Env: []corev1.EnvVar{
					{
						Name: "SECRET_ENV",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
							},
						},
					},
				},
			},
			expected: []string{"my-secret"},
		},
		{
			name: "Secrets in EnvFrom",
			container: corev1.Container{
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-envfrom-secret"},
						},
					},
				},
			},
			expected: []string{"my-envfrom-secret"},
		},
		{
			name: "Secrets in both Env and EnvFrom",
			container: corev1.Container{
				Env: []corev1.EnvVar{
					{
						Name: "SECRET_ENV",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
							},
						},
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-envfrom-secret"},
						},
					},
				},
			},
			expected: []string{"my-secret", "my-envfrom-secret"},
		},
		{
			name: "Duplicate secrets are deduplicated",
			container: corev1.Container{
				Env: []corev1.EnvVar{
					{
						Name: "SECRET_ENV",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "duplicate-secret"},
							},
						},
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "duplicate-secret"},
						},
					},
				},
			},
			expected: []string{"duplicate-secret"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := mapset.NewSet[string]()
			containerSecretNamesFromEnv(seen, tt.container)
			assert.ElementsMatch(t, tt.expected, seen.ToSlice())
		})
	}
}

func TestContainerSecretNamesFromVolumes(t *testing.T) {
	tests := []struct {
		name       string
		podVolumes []corev1.Volume
		container  corev1.Container
		expected   []string
	}{
		{
			name:       "No secrets in volumes",
			podVolumes: []corev1.Volume{},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{},
			},
			expected: []string{},
		},
		{
			name: "Secret in volume",
			podVolumes: []corev1.Volume{
				{
					Name: "my-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "my-secret",
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "my-volume"},
				},
			},
			expected: []string{"my-secret"},
		},
		{
			name: "Multiple secrets in volumes",
			podVolumes: []corev1.Volume{
				{
					Name: "volume-1",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "secret-1",
						},
					},
				},
				{
					Name: "volume-2",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "secret-2",
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "volume-1"},
					{Name: "volume-2"},
				},
			},
			expected: []string{"secret-1", "secret-2"},
		},
		{
			name: "Ignore default-token secret",
			podVolumes: []corev1.Volume{
				{
					Name: "default-token-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "default-token-abc123",
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "default-token-volume"},
				},
			},
			expected: []string{},
		},
		{
			name: "Mixed valid and default-token secrets",
			podVolumes: []corev1.Volume{
				{
					Name: "valid-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "valid-secret",
						},
					},
				},
				{
					Name: "default-token-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "default-token-abc123",
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "valid-volume"},
					{Name: "default-token-volume"},
				},
			},
			expected: []string{"valid-secret"},
		},
		{
			name: "Duplicate secrets are deduplicated",
			podVolumes: []corev1.Volume{
				{
					Name: "volume-1",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "duplicate-secret",
						},
					},
				},
				{
					Name: "volume-2",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "duplicate-secret",
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "volume-1"},
					{Name: "volume-2"},
				},
			},
			expected: []string{"duplicate-secret"},
		},
	}

	conf := converterconfig.New("foo")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := mapset.NewSet[string]()
			containerSecretNamesFromVolumes(conf, seen, tt.podVolumes, tt.container)
			assert.ElementsMatch(t, tt.expected, seen.ToSlice())
		})
	}
}

func TestContainerConfigMapNamesFromVolumes(t *testing.T) {
	tests := []struct {
		name       string
		podVolumes []corev1.Volume
		container  corev1.Container
		expected   []string
	}{
		{
			name:       "No config maps in volumes",
			podVolumes: []corev1.Volume{},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{},
			},
			expected: []string{},
		},
		{
			name: "Config map in volume",
			podVolumes: []corev1.Volume{
				{
					Name: "my-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "my-volume"},
				},
			},
			expected: []string{"my-configmap"},
		},
		{
			name: "Multiple config maps in volumes",
			podVolumes: []corev1.Volume{
				{
					Name: "volume-1",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "configmap-1"},
						},
					},
				},
				{
					Name: "volume-2",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "configmap-2"},
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "volume-1"},
					{Name: "volume-2"},
				},
			},
			expected: []string{"configmap-1", "configmap-2"},
		},
		{
			name: "Ignore kube-root-ca.crt config map",
			podVolumes: []corev1.Volume{
				{
					Name: "ca-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "ca-volume"},
				},
			},
			expected: []string{},
		},
		{
			name: "Mixed valid and kube-root-ca.crt config maps",
			podVolumes: []corev1.Volume{
				{
					Name: "valid-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "valid-configmap"},
						},
					},
				},
				{
					Name: "ca-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "valid-volume"},
					{Name: "ca-volume"},
				},
			},
			expected: []string{"valid-configmap"},
		},
		{
			name: "Duplicate config maps are deduplicated",
			podVolumes: []corev1.Volume{
				{
					Name: "volume-1",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "duplicate-configmap"},
						},
					},
				},
				{
					Name: "volume-2",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "duplicate-configmap"},
						},
					},
				},
			},
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "volume-1"},
					{Name: "volume-2"},
				},
			},
			expected: []string{"duplicate-configmap"},
		},
	}

	conf := converterconfig.New("foo")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := mapset.NewSet[string]()
			containerConfigMapNamesFromVolumes(conf, seen, tt.podVolumes, tt.container)
			assert.ElementsMatch(t, tt.expected, seen.ToSlice())
		})
	}
}

func TestContainerConfigMapNamesFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		container corev1.Container
		expected  []string
	}{
		{
			name: "No config maps in env",
			container: corev1.Container{
				Env:     []corev1.EnvVar{},
				EnvFrom: []corev1.EnvFromSource{},
			},
			expected: []string{},
		},
		{
			name: "Config map in Env",
			container: corev1.Container{
				Env: []corev1.EnvVar{
					{
						Name: "CONFIG_ENV",
						ValueFrom: &corev1.EnvVarSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
							},
						},
					},
				},
			},
			expected: []string{"my-configmap"},
		},
		{
			name: "Config map in EnvFrom",
			container: corev1.Container{
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-envfrom-configmap"},
						},
					},
				},
			},
			expected: []string{"my-envfrom-configmap"},
		},
		{
			name: "Config maps in both Env and EnvFrom",
			container: corev1.Container{
				Env: []corev1.EnvVar{
					{
						Name: "CONFIG_ENV",
						ValueFrom: &corev1.EnvVarSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
							},
						},
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-envfrom-configmap"},
						},
					},
				},
			},
			expected: []string{"my-configmap", "my-envfrom-configmap"},
		},
		{
			name: "Duplicate config maps are deduplicated",
			container: corev1.Container{
				Env: []corev1.EnvVar{
					{
						Name: "CONFIG_ENV",
						ValueFrom: &corev1.EnvVarSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "duplicate-configmap"},
							},
						},
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "duplicate-configmap"},
						},
					},
				},
			},
			expected: []string{"duplicate-configmap"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := mapset.NewSet[string]()
			containerConfigMapNamesFromEnv(seen, tt.container)
			assert.ElementsMatch(t, tt.expected, seen.ToSlice())
		})
	}
}

func TestSetupContainers(t *testing.T) {
	tests := []struct {
		name     string
		pod      corev1.Pod
		expected []*graphpb.PodContainerSpec
	}{
		{
			name: "Single container with resources, config maps, and secrets",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "test-image",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "CONFIG_ENV",
									ValueFrom: &corev1.EnvVarSource{
										ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
										},
									},
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "config-volume"},
								{Name: "secret-volume"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
								},
							},
						},
						{
							Name: "secret-volume",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: "my-secret",
								},
							},
						},
					},
				},
			},
			expected: []*graphpb.PodContainerSpec{
				{
					Name:  "test-container",
					Image: "test-image",
					Resources: map[string]string{
						"requests.cpu":    "500m",
						"requests.memory": "128Mi",
						"limits.cpu":      "1",
						"limits.memory":   "256Mi",
					},
					ConfigMapNames: []string{"my-configmap"},
					SecretNames:    []string{"my-secret"},
				},
			},
		},
		{
			name: "Multiple containers sorted by name",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "b-container",
							Image: "b-image",
						},
						{
							Name:  "a-container",
							Image: "a-image",
						},
					},
				},
			},
			expected: []*graphpb.PodContainerSpec{
				{
					Name:  "a-container",
					Image: "a-image",
				},
				{
					Name:  "b-container",
					Image: "b-image",
				},
			},
		},
		{
			name: "No containers",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{},
				},
			},
			expected: []*graphpb.PodContainerSpec{},
		},
	}

	conf := &converterconfig.Config{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			containers := GetContainerSpecs(conf, tt.pod.Spec)
			assert.Equal(t, tt.expected, containers)
		})
	}
}

func TestContainerPorts(t *testing.T) {
	tests := []struct {
		name      string
		container corev1.Container
		expected  []*graphpb.ContainerPortSpec
	}{
		{
			name: "No ports",
			container: corev1.Container{
				Ports: []corev1.ContainerPort{},
			},
			expected: nil,
		},
		{
			name: "Single port",
			container: corev1.Container{
				Ports: []corev1.ContainerPort{
					{
						Name:          "http",
						ContainerPort: 80,
						Protocol:      corev1.ProtocolTCP,
					},
				},
			},
			expected: []*graphpb.ContainerPortSpec{
				{
					Name:          "http",
					ContainerPort: 80,
					Protocol:      "TCP",
				},
			},
		},
		{
			name: "Multiple ports sorted by name",
			container: corev1.Container{
				Ports: []corev1.ContainerPort{
					{
						Name:          "z-port",
						ContainerPort: 8080,
						Protocol:      corev1.ProtocolTCP,
					},
					{
						Name:          "a-port",
						ContainerPort: 9090,
						Protocol:      corev1.ProtocolUDP,
					},
				},
			},
			expected: []*graphpb.ContainerPortSpec{
				{
					Name:          "a-port",
					ContainerPort: 9090,
					Protocol:      "UDP",
				},
				{
					Name:          "z-port",
					ContainerPort: 8080,
					Protocol:      "TCP",
				},
			},
		},
		{
			name: "Ports with zero container port are ignored",
			container: corev1.Container{
				Ports: []corev1.ContainerPort{
					{
						Name:          "valid-port",
						ContainerPort: 8080,
						Protocol:      corev1.ProtocolTCP,
					},
					{
						Name:          "zero-port",
						ContainerPort: 0,
						Protocol:      corev1.ProtocolTCP,
					},
				},
			},
			expected: []*graphpb.ContainerPortSpec{
				{
					Name:          "valid-port",
					ContainerPort: 8080,
					Protocol:      "TCP",
				},
			},
		},
		{
			name: "Ports without names are sorted by default order",
			container: corev1.Container{
				Ports: []corev1.ContainerPort{
					{
						ContainerPort: 8080,
						Protocol:      corev1.ProtocolTCP,
					},
					{
						ContainerPort: 9090,
						Protocol:      corev1.ProtocolUDP,
					},
				},
			},
			expected: []*graphpb.ContainerPortSpec{
				{
					Name:          "",
					ContainerPort: 8080,
					Protocol:      "TCP",
				},
				{
					Name:          "",
					ContainerPort: 9090,
					Protocol:      "UDP",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containerPorts(tt.container)
			assert.Equal(t, tt.expected, result)
		})
	}
}
