package v1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type PodSummary struct {
	Name       string             `json:"name"`
	Namespace  string             `json:"namespace"`
	Containers []ContainerSummary `json:"containers"`
	HostIPs    []string           `json:"host_ips"`
	PodIPs     []string           `json:"pod_ips"`
}

type ContainerSummary struct {
	Name  string       `json:"name"`
	Image ImageSummary `json:"image"`
}

type ImageSummary struct {
	Image   string `json:"name"`
	ImageID string `json:"image_id"`
}

func ConvertPod(us unstructured.Unstructured) (*PodSummary, error) {
	var pod corev1.Pod
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &pod)
	if err != nil {
		return nil, err
	}

	podSummary := &PodSummary{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}

	for _, container := range pod.Spec.Containers {
		podSummary.Containers = append(podSummary.Containers, ContainerSummary{
			Name: container.Name,
			Image: ImageSummary{
				Image: container.Image,
			},
		})
	}

	for _, status := range pod.Status.ContainerStatuses {
		for i, container := range podSummary.Containers {
			if container.Name == status.Name {
				podSummary.Containers[i].Image.ImageID = status.ImageID
				if container.Image.Image != status.Image && status.Image != "" {
					podSummary.Containers[i].Image.Image = status.Image
				}
			}
		}
	}

	for _, hostip := range pod.Status.HostIPs {
		podSummary.HostIPs = append(podSummary.HostIPs, hostip.IP)
	}

	for _, podip := range pod.Status.PodIPs {
		podSummary.PodIPs = append(podSummary.PodIPs, podip.IP)
	}

	return podSummary, nil
}
