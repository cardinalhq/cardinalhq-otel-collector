package v1

import (
	"testing"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := mapset.NewSet[string]()
			containerSecretNamesFromVolumes(seen, tt.podVolumes, tt.container)
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := mapset.NewSet[string]()
			containerConfigMapNamesFromVolumes(seen, tt.podVolumes, tt.container)
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

func TestConvertPod(t *testing.T) {
	tests := []struct {
		name     string
		input    unstructured.Unstructured
		expected *PodSummary
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
					},
				},
			},
			expected: &PodSummary{
				BaseObject: baseobj.BaseObject{
					Name:      "test-pod",
					Namespace: "default",
				},
				Phase:              "Running",
				PhaseMessage:       "Pod is running",
				ServiceAccountName: "test-service-account",
				StartedAt:          time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				Containers: []PodContainerSummary{
					{
						Name:  "test-container",
						Image: ImageSummary{Image: "test-image"},
					},
				},
				HostIPs: []string{"192.168.1.1"},
				PodIPs:  []string{"10.0.0.1"},
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
					},
				},
			},
			expected: &PodSummary{
				Phase: "Pending",
				Containers: []PodContainerSummary{
					{
						Name:  "test-container",
						Image: ImageSummary{Image: "test-image"},
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertPod(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.expected.BaseObject = baseobj.BaseFromUnstructured(tt.input)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSetupContainers(t *testing.T) {
	tests := []struct {
		name     string
		pod      corev1.Pod
		expected []PodContainerSummary
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
			expected: []PodContainerSummary{
				{
					Name:  "test-container",
					Image: ImageSummary{Image: "test-image"},
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
			expected: []PodContainerSummary{
				{
					Name:  "a-container",
					Image: ImageSummary{Image: "a-image"},
				},
				{
					Name:  "b-container",
					Image: ImageSummary{Image: "b-image"},
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
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podSummary := &PodSummary{}
			setupContainers(tt.pod, podSummary)
			assert.Equal(t, tt.expected, podSummary.Containers)
		})
	}
}
